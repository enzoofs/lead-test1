"""
Extrator de redes sociais a partir do site do lead

Este modulo visita o site do lead e extrai links para redes sociais.
E a forma mais eficiente e gratuita de obter esses dados.
"""
import re
import time
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
    Extrai perfis de redes sociais do site do lead

    Estrategia:
    1. Visita o site principal
    2. Procura links para redes sociais no HTML
    3. Verifica paginas comuns (contato, sobre)
    4. Extrai e valida os perfis encontrados
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

    # Paginas comuns onde encontrar redes sociais
    COMMON_PAGES = [
        "",  # Homepage
        "/contato",
        "/contact",
        "/sobre",
        "/about",
        "/fale-conosco",
    ]

    def __init__(self):
        self.session = httpx.Client(
            timeout=TIMEOUT_SECONDS,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            },
            follow_redirects=True,
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

    def extract(self, lead: Lead) -> Lead:
        """
        Extrai redes sociais do site do lead

        Args:
            lead: Lead com site preenchido

        Returns:
            Lead atualizado com perfis sociais
        """
        if not lead.site:
            logger.debug(f"Lead {lead.nome} sem site, pulando")
            return lead

        logger.info(f"Extraindo redes sociais de {lead.site}")

        social = SocialProfiles()
        all_links = set()

        # Normalizar URL base
        base_url = self._normalize_url(lead.site)

        # Buscar em paginas comuns
        for page in self.COMMON_PAGES:
            url = urljoin(base_url, page)
            html = self._fetch_page(url)

            if html:
                links = self._extract_links(html, base_url)
                all_links.update(links)

            time.sleep(0.5)  # Rate limiting

        # Processar links encontrados
        social = self._parse_social_links(all_links)

        # Tentar extrair email tambem
        email = self._extract_email(all_links)
        if email and not lead.email:
            lead.email = email

        # Atualizar lead
        lead.social = social
        lead.social_enriched = True
        lead.site_ativo = True

        logger.info(
            f"Lead {lead.nome}: Instagram={social.instagram}, "
            f"LinkedIn={social.linkedin}"
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

    def _extract_email(self, links: set[str]) -> Optional[str]:
        """Extrai email dos links mailto:"""
        for link in links:
            if link.startswith("mailto:"):
                email = link.replace("mailto:", "").split("?")[0]
                if "@" in email and "." in email:
                    return email.lower()

        return None

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
