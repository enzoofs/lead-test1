"""
Extrator de redes sociais, emails e telefones a partir do site do lead

Este modulo visita o site do lead e extrai:
- Links para redes sociais (Instagram, LinkedIn, Facebook, etc)
- Emails (mailto: e texto)
- Telefones (links tel: e texto)
"""
import re
import time
import ssl
import structlog
import httpx
from typing import Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import USER_AGENT, TIMEOUT_SECONDS, MAX_RETRIES
from src.models import Lead, SocialProfiles

logger = structlog.get_logger()


class SocialMediaExtractor:
    """
    Extrai perfis de redes sociais, emails e telefones do site do lead

    Estrategia:
    1. Verifica se o "site" e na verdade um link do Instagram
    2. Visita o site principal
    3. Procura links para redes sociais no HTML
    4. Extrai emails e telefones do HTML
    5. Verifica paginas comuns (contato, sobre)
    """

    # Padroes de URLs de redes sociais
    SOCIAL_PATTERNS = {
        "instagram": [
            r"(?:https?://)?(?:www\.)?instagram\.com/([a-zA-Z0-9_.]+)/?",
            r"(?:https?://)?(?:www\.)?instagr\.am/([a-zA-Z0-9_.]+)/?",
        ],
        "linkedin": [
            r"(?:https?://)?(?:www\.)?linkedin\.com/company/([a-zA-Z0-9-]+)/?",
            r"(?:https?://)?(?:www\.)?linkedin\.com/in/([a-zA-Z0-9-]+)/?",
            r"(?:https?://)?(?:br\.)?linkedin\.com/company/([a-zA-Z0-9-]+)/?",
        ],
        "facebook": [
            r"(?:https?://)?(?:www\.)?facebook\.com/([a-zA-Z0-9.]+)/?",
            r"(?:https?://)?(?:www\.)?fb\.com/([a-zA-Z0-9.]+)/?",
        ],
        "twitter": [
            r"(?:https?://)?(?:www\.)?twitter\.com/([a-zA-Z0-9_]+)/?",
            r"(?:https?://)?(?:www\.)?x\.com/([a-zA-Z0-9_]+)/?",
        ],
        "youtube": [
            r"(?:https?://)?(?:www\.)?youtube\.com/(?:c/|channel/|user/)?([a-zA-Z0-9_-]+)/?",
        ],
        "whatsapp": [
            r"(?:https?://)?(?:api\.)?whatsapp\.com/send\?phone=(\d+)",
            r"(?:https?://)?wa\.me/(\d+)",
        ],
    }

    # Padroes para extrair emails do texto
    EMAIL_PATTERNS = [
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    ]

    # Padroes para extrair telefones brasileiros
    PHONE_PATTERNS = [
        # Formato com DDD: (31) 99999-9999 ou (31) 3333-3333
        r'\(?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4}',
        # Formato internacional: +55 31 99999-9999
        r'\+55\s*\d{2}\s*\d{4,5}[-.\s]?\d{4}',
        # Links tel:
        r'tel:[\+\d\s\-\(\)]+',
    ]

    # Paginas comuns onde encontrar redes sociais e contato
    COMMON_PAGES = [
        "",  # Homepage
        "/contato",
        "/contact",
        "/sobre",
        "/about",
        "/fale-conosco",
    ]

    # Dominios de redes sociais (para detectar quando site e rede social)
    SOCIAL_DOMAINS = [
        "instagram.com", "instagr.am",
        "facebook.com", "fb.com",
        "linkedin.com",
        "twitter.com", "x.com",
        "wa.me", "whatsapp.com",
        "l.instagram.com",  # Redirect do Instagram
    ]

    def __init__(self):
        # Criar cliente HTTP com tolerancia a erros SSL
        self.session = httpx.Client(
            timeout=TIMEOUT_SECONDS,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            },
            follow_redirects=True,
            verify=False,  # Tolerar certificados invalidos
        )

    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=1, max=5),
    )
    def _fetch_page(self, url: str) -> Optional[str]:
        """Busca pagina com retry"""
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                return response.text
        except Exception as e:
            logger.warning(f"Erro ao buscar {url}: {e}")
        return None

    def _is_social_media_url(self, url: str) -> bool:
        """Verifica se a URL e de uma rede social"""
        if not url:
            return False
        url_lower = url.lower()
        return any(domain in url_lower for domain in self.SOCIAL_DOMAINS)

    def _extract_instagram_from_url(self, url: str) -> Optional[str]:
        """Extrai username do Instagram de uma URL"""
        for pattern in self.SOCIAL_PATTERNS["instagram"]:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                username = match.group(1)
                if username not in ["p", "reel", "stories", "explore", ""]:
                    return username
        return None

    def extract(self, lead: Lead) -> Lead:
        """
        Extrai redes sociais, email e telefone do site do lead

        Args:
            lead: Lead com site preenchido

        Returns:
            Lead atualizado com perfis sociais, email e telefone
        """
        if not lead.site:
            logger.debug(f"Lead {lead.nome} sem site, pulando")
            return lead

        logger.info(f"Extraindo redes sociais de {lead.site}")

        social = SocialProfiles()
        all_links = set()
        all_text = ""

        # Verificar se o "site" e na verdade um link do Instagram
        if self._is_social_media_url(lead.site):
            instagram_user = self._extract_instagram_from_url(lead.site)
            if instagram_user:
                social.instagram = f"https://instagram.com/{instagram_user}"
                logger.info(f"Site e Instagram: @{instagram_user}")

            # Marcar que nao tem site real
            lead.site_ativo = False
            lead.social = social
            lead.social_enriched = True
            return lead

        # Normalizar URL base
        base_url = self._normalize_url(lead.site)

        # Buscar em paginas comuns
        for page in self.COMMON_PAGES:
            url = urljoin(base_url, page)
            html = self._fetch_page(url)

            if html:
                links = self._extract_links(html, base_url)
                all_links.update(links)

                # Acumular texto para extrair emails/telefones
                try:
                    soup = BeautifulSoup(html, "lxml")
                    all_text += " " + soup.get_text(separator=" ")
                except Exception:
                    pass

            time.sleep(0.5)  # Rate limiting

        # Processar links encontrados
        social = self._parse_social_links(all_links)

        # Extrair email (de mailto: ou do texto)
        email = self._extract_email(all_links, all_text)
        if email and not lead.email:
            lead.email = email

        # Extrair telefone do texto
        telefone = self._extract_phone(all_links, all_text)
        if telefone and not lead.telefone:
            lead.telefone = telefone

        # Atualizar lead
        lead.social = social
        lead.social_enriched = True
        lead.site_ativo = True

        logger.info(
            f"Lead {lead.nome}: Instagram={social.instagram}, "
            f"LinkedIn={social.linkedin}, Email={lead.email}, Tel={lead.telefone}"
        )

        return lead

    def _normalize_url(self, url: str) -> str:
        """Normaliza URL adicionando schema se necessario"""
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        # Remover trailing slash
        url = url.rstrip("/")

        return url

    def _extract_links(self, html: str, base_url: str) -> set[str]:
        """Extrai todos os links de uma pagina"""
        links = set()

        try:
            soup = BeautifulSoup(html, "lxml")

            # Links em tags <a>
            for a in soup.find_all("a", href=True):
                href = a["href"]
                # Converter links relativos
                full_url = urljoin(base_url, href)
                links.add(full_url)

            # Links em texto (algumas paginas colocam URLs em texto)
            text = soup.get_text()
            url_pattern = r'https?://[^\s<>"\']+|www\.[^\s<>"\']+'
            for match in re.findall(url_pattern, text):
                links.add(match)

        except Exception as e:
            logger.warning(f"Erro ao extrair links: {e}")

        return links

    def _parse_social_links(self, links: set[str]) -> SocialProfiles:
        """Identifica e extrai perfis sociais dos links"""
        social = SocialProfiles()

        for link in links:
            link_lower = link.lower()

            # Instagram
            if "instagram.com" in link_lower or "instagr.am" in link_lower:
                for pattern in self.SOCIAL_PATTERNS["instagram"]:
                    match = re.search(pattern, link, re.IGNORECASE)
                    if match:
                        username = match.group(1)
                        if username not in ["p", "reel", "stories", "explore"]:
                            social.instagram = f"https://instagram.com/{username}"
                            break

            # LinkedIn
            elif "linkedin.com" in link_lower:
                for pattern in self.SOCIAL_PATTERNS["linkedin"]:
                    match = re.search(pattern, link, re.IGNORECASE)
                    if match:
                        identifier = match.group(1)
                        if "company" in link_lower:
                            social.linkedin = f"https://linkedin.com/company/{identifier}"
                            social.linkedin_company_id = identifier
                        else:
                            social.linkedin = f"https://linkedin.com/in/{identifier}"
                        break

            # Facebook
            elif "facebook.com" in link_lower or "fb.com" in link_lower:
                for pattern in self.SOCIAL_PATTERNS["facebook"]:
                    match = re.search(pattern, link, re.IGNORECASE)
                    if match:
                        page = match.group(1)
                        if page not in ["sharer", "share", "dialog"]:
                            social.facebook = f"https://facebook.com/{page}"
                            break

            # Twitter/X
            elif "twitter.com" in link_lower or "x.com" in link_lower:
                for pattern in self.SOCIAL_PATTERNS["twitter"]:
                    match = re.search(pattern, link, re.IGNORECASE)
                    if match:
                        username = match.group(1)
                        if username not in ["share", "intent", "home"]:
                            social.twitter = f"https://twitter.com/{username}"
                            break

            # YouTube
            elif "youtube.com" in link_lower:
                for pattern in self.SOCIAL_PATTERNS["youtube"]:
                    match = re.search(pattern, link, re.IGNORECASE)
                    if match:
                        channel = match.group(1)
                        if channel not in ["watch", "results", "playlist"]:
                            social.youtube = f"https://youtube.com/{channel}"
                            break

        return social

    def _extract_email(self, links: set[str], text: str = "") -> Optional[str]:
        """
        Extrai email de:
        1. Links mailto:
        2. Texto da pagina

        Prioriza emails corporativos (contato@, comercial@, etc)
        """
        found_emails = []

        # 1. Buscar em links mailto:
        for link in links:
            if link.startswith("mailto:"):
                email = link.replace("mailto:", "").split("?")[0].strip()
                if "@" in email and "." in email:
                    found_emails.append(email.lower())

        # 2. Buscar no texto da pagina
        for pattern in self.EMAIL_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for email in matches:
                email = email.lower().strip()
                # Filtrar emails invalidos
                if self._is_valid_email(email):
                    found_emails.append(email)

        if not found_emails:
            return None

        # Remover duplicatas mantendo ordem
        found_emails = list(dict.fromkeys(found_emails))

        # Priorizar emails corporativos
        priority_prefixes = ["contato", "comercial", "info", "atendimento", "vendas", "sac"]
        for email in found_emails:
            prefix = email.split("@")[0]
            if any(p in prefix for p in priority_prefixes):
                return email

        # Retornar primeiro email encontrado
        return found_emails[0] if found_emails else None

    def _is_valid_email(self, email: str) -> bool:
        """Valida se email parece legitimo"""
        if not email or "@" not in email:
            return False

        # Ignorar emails de exemplo ou placeholders
        invalid_patterns = [
            "example.com", "teste.com", "email.com",
            "seudominio", "yourdomain", "domain.com",
            ".png", ".jpg", ".gif", ".css", ".js",
            "wixpress", "wordpress",
        ]
        return not any(p in email.lower() for p in invalid_patterns)

    def _extract_phone(self, links: set[str], text: str = "") -> Optional[str]:
        """
        Extrai telefone de:
        1. Links tel:
        2. Texto da pagina

        Formata para padrao brasileiro
        """
        found_phones = []

        # 1. Buscar em links tel:
        for link in links:
            if link.startswith("tel:"):
                phone = link.replace("tel:", "").strip()
                phone = self._normalize_phone(phone)
                if phone:
                    found_phones.append(phone)

        # 2. Buscar no texto da pagina
        for pattern in self.PHONE_PATTERNS:
            matches = re.findall(pattern, text)
            for phone in matches:
                phone = self._normalize_phone(phone)
                if phone:
                    found_phones.append(phone)

        if not found_phones:
            return None

        # Remover duplicatas mantendo ordem
        found_phones = list(dict.fromkeys(found_phones))

        # Priorizar celulares (comecam com 9)
        for phone in found_phones:
            digits = re.sub(r'\D', '', phone)
            # Celular tem 11 digitos e o 5o digito e 9
            if len(digits) >= 10:
                if len(digits) == 11 and digits[2] == '9':
                    return phone
                elif len(digits) == 10 and digits[2] == '9':
                    return phone

        return found_phones[0] if found_phones else None

    def _normalize_phone(self, phone: str) -> Optional[str]:
        """Normaliza telefone para formato padrao"""
        if not phone:
            return None

        # Remover caracteres nao numericos exceto +
        digits = re.sub(r'[^\d+]', '', phone)

        # Remover +55 do inicio se presente
        if digits.startswith('+55'):
            digits = digits[3:]
        elif digits.startswith('55') and len(digits) > 11:
            digits = digits[2:]

        # Validar tamanho (10 ou 11 digitos para BR)
        if len(digits) < 10 or len(digits) > 11:
            return None

        # Formatar: (XX) XXXXX-XXXX ou (XX) XXXX-XXXX
        if len(digits) == 11:
            return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
        else:
            return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"

    def enrich_leads(self, leads: list[Lead]) -> list[Lead]:
        """
        Enriquece lista de leads com redes sociais

        Args:
            leads: Lista de leads para enriquecer

        Returns:
            Lista de leads enriquecidos
        """
        enriched = []
        total = len(leads)

        for i, lead in enumerate(leads, 1):
            logger.info(f"Processando {i}/{total}: {lead.nome}")

            try:
                enriched_lead = self.extract(lead)
                enriched.append(enriched_lead)
            except Exception as e:
                logger.error(f"Erro ao enriquecer {lead.nome}: {e}")
                enriched.append(lead)

            # Rate limiting
            time.sleep(1)

        return enriched
