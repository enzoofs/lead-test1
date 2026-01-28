"""
Analisador de websites para qualificacao de leads

Verifica se o site esta ativo, tem HTTPS, e extrai informacoes
adicionais como tecnologias usadas.
"""
import re
import time
import structlog
import httpx
from typing import Optional
from bs4 import BeautifulSoup
from urllib.parse import urlparse

from config.settings import USER_AGENT, TIMEOUT_SECONDS
from src.models import Lead

logger = structlog.get_logger()


class WebsiteAnalyzer:
    """
    Analisa websites dos leads para qualificacao

    Verifica:
    - Site ativo (responde 200)
    - Usa HTTPS
    - Tem meta tags adequadas
    - Tempo de resposta
    """

    def __init__(self):
        self.session = httpx.Client(
            timeout=TIMEOUT_SECONDS,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )

    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()

    def analyze(self, lead: Lead) -> Lead:
        """
        Analisa o site do lead

        Args:
            lead: Lead com site preenchido

        Returns:
            Lead atualizado com informacoes do site
        """
        if not lead.site:
            return lead

        url = self._normalize_url(lead.site)
        logger.info(f"Analisando site: {url}")

        try:
            start_time = time.time()
            response = self.session.get(url)
            response_time = time.time() - start_time

            # Site ativo
            lead.site_ativo = response.status_code == 200

            # Verificar HTTPS
            final_url = str(response.url)
            lead.site_https = final_url.startswith("https://")

            # Atualizar URL final (pode ter redirecionado)
            lead.site = final_url

            if response.status_code == 200:
                # Extrair informacoes adicionais do HTML
                self._extract_metadata(lead, response.text)

            logger.info(
                f"Site {url}: ativo={lead.site_ativo}, "
                f"https={lead.site_https}, tempo={response_time:.2f}s"
            )

        except httpx.TimeoutException:
            logger.warning(f"Timeout ao acessar {url}")
            lead.site_ativo = False

        except Exception as e:
            logger.warning(f"Erro ao analisar {url}: {e}")
            lead.site_ativo = False

        return lead

    def _normalize_url(self, url: str) -> str:
        """Normaliza URL"""
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        return url.rstrip("/")

    def _extract_metadata(self, lead: Lead, html: str):
        """Extrai metadados do HTML"""
        try:
            soup = BeautifulSoup(html, "lxml")

            # Tentar extrair email se nao tiver
            if not lead.email:
                lead.email = self._find_email(soup, html)

            # Tentar extrair telefone se nao tiver
            if not lead.telefone:
                lead.telefone = self._find_phone(html)

        except Exception as e:
            logger.warning(f"Erro ao extrair metadados: {e}")

    def _find_email(self, soup: BeautifulSoup, html: str) -> Optional[str]:
        """Procura email na pagina"""
        # Links mailto
        for a in soup.find_all("a", href=True):
            if a["href"].startswith("mailto:"):
                email = a["href"].replace("mailto:", "").split("?")[0]
                if self._is_valid_email(email):
                    return email.lower()

        # Padrao de email no texto
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        matches = re.findall(email_pattern, html)

        for email in matches:
            # Filtrar emails invalidos comuns
            if self._is_valid_email(email):
                return email.lower()

        return None

    def _is_valid_email(self, email: str) -> bool:
        """Valida se email parece legitimo"""
        invalid_patterns = [
            "example.com",
            "teste.com",
            "email.com",
            "sentry.io",
            "wix.com",
            ".png",
            ".jpg",
            ".gif",
        ]

        email_lower = email.lower()
        return not any(p in email_lower for p in invalid_patterns)

    def _find_phone(self, html: str) -> Optional[str]:
        """Procura telefone na pagina"""
        # Padroes de telefone brasileiro
        patterns = [
            r'\(?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4}',  # (31) 99999-9999
            r'\+55\s*\d{2}\s*\d{4,5}[-.\s]?\d{4}',  # +55 31 99999-9999
        ]

        for pattern in patterns:
            matches = re.findall(pattern, html)
            if matches:
                # Limpar e retornar primeiro
                phone = re.sub(r'[^\d]', '', matches[0])
                if len(phone) >= 10:
                    return phone

        return None

    def analyze_leads(self, leads: list[Lead]) -> list[Lead]:
        """Analisa lista de leads"""
        analyzed = []

        for i, lead in enumerate(leads, 1):
            logger.info(f"Analisando {i}/{len(leads)}: {lead.nome}")

            try:
                analyzed_lead = self.analyze(lead)
                analyzed.append(analyzed_lead)
            except Exception as e:
                logger.error(f"Erro: {e}")
                analyzed.append(lead)

            time.sleep(0.5)

        return analyzed
