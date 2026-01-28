"""
Scraper do Google Maps usando requests + BeautifulSoup
Alternativa gratuita ao SerpAPI (menos confiavel)
"""
import re
import time
import json
import structlog
import httpx
from typing import Optional
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import (
    SEARCH_LOCATION,
    USER_AGENT,
    MAX_RETRIES,
    TIMEOUT_SECONDS,
    DELAY_BETWEEN_REQUESTS,
)
from src.models import Lead, SearchQuery, ScrapingResult, GoogleMapsData

logger = structlog.get_logger()


class GoogleMapsScraper:
    """
    Scraper direto do Google Maps

    AVISO: Esta abordagem pode resultar em bloqueios.
    Use SerpAPI para producao.
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

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def _fetch_page(self, url: str) -> str:
        """Busca pagina com retry automatico"""
        response = self.session.get(url)
        response.raise_for_status()
        return response.text

    def search(self, query: SearchQuery) -> ScrapingResult:
        """
        Busca negocios no Google Maps

        NOTA: Este metodo usa scraping direto e pode ser instavel.
        Para producao, use GoogleMapsSerpAPI.
        """
        start_time = time.time()
        leads = []
        errors = []

        try:
            search_query = f"{query.query} {query.location}"
            encoded_query = search_query.replace(" ", "+")

            # URL do Google Maps search
            url = f"https://www.google.com/maps/search/{encoded_query}"

            logger.info("Buscando no Google Maps", url=url)
            time.sleep(DELAY_BETWEEN_REQUESTS)

            html = self._fetch_page(url)

            # Google Maps usa JavaScript pesado, entao o HTML
            # retornado contem dados em formato JSON embedado
            leads = self._extract_from_html(html, query.category)

            logger.info(f"Extraidos {len(leads)} leads")

        except Exception as e:
            error_msg = f"Erro no scraping: {str(e)}"
            errors.append(error_msg)
            logger.error(error_msg)

        duration = time.time() - start_time

        return ScrapingResult(
            success=len(errors) == 0,
            leads=leads,
            errors=errors,
            total_found=len(leads),
            query=query,
            duration_seconds=duration,
        )

    def _extract_from_html(self, html: str, category: str) -> list[Lead]:
        """
        Extrai dados do HTML do Google Maps

        O Google Maps embedda dados JSON no HTML que podemos extrair.
        """
        leads = []

        # Tentar encontrar dados JSON embedados
        # O Google usa varios padroes, tentamos os mais comuns
        patterns = [
            r'window\.APP_INITIALIZATION_STATE=(\[.*?\]);',
            r'null,null,(\[.*?\])\],"',
            r'"features":(\[.*?\]),'
        ]

        for pattern in patterns:
            matches = re.findall(pattern, html, re.DOTALL)
            if matches:
                try:
                    data = json.loads(matches[0])
                    leads = self._parse_json_data(data, category)
                    if leads:
                        break
                except json.JSONDecodeError:
                    continue

        # Fallback: parsing HTML tradicional
        if not leads:
            leads = self._parse_html_fallback(html, category)

        return leads

    def _parse_json_data(self, data: list, category: str) -> list[Lead]:
        """Parse dados JSON extraidos do Google Maps"""
        leads = []

        def extract_business(item):
            """Extrai recursivamente dados de negocio"""
            if isinstance(item, list):
                for sub_item in item:
                    extract_business(sub_item)
            elif isinstance(item, dict):
                # Verificar se parece com dados de negocio
                if "title" in item or "name" in item:
                    try:
                        lead = Lead(
                            nome=item.get("title") or item.get("name", ""),
                            categoria=category,
                            telefone=item.get("phone"),
                            endereco=item.get("address"),
                            site=item.get("website"),
                            google_maps=GoogleMapsData(
                                rating=item.get("rating"),
                                num_reviews=item.get("reviews"),
                            ),
                        )
                        if lead.nome:
                            leads.append(lead)
                    except Exception:
                        pass
                for value in item.values():
                    extract_business(value)

        extract_business(data)
        return leads

    def _parse_html_fallback(self, html: str, category: str) -> list[Lead]:
        """
        Fallback: extrai dados usando BeautifulSoup

        NOTA: Google Maps e altamente dinamico, este metodo
        tem baixa taxa de sucesso.
        """
        leads = []
        soup = BeautifulSoup(html, "lxml")

        # Tentar encontrar cards de negocios
        # Os seletores podem mudar frequentemente
        selectors = [
            "div[role='article']",
            "div.Nv2PK",
            "a[href*='/maps/place/']",
        ]

        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                for elem in elements:
                    try:
                        nome = elem.get_text(strip=True)[:100]
                        if nome:
                            lead = Lead(
                                nome=nome,
                                categoria=category,
                            )
                            leads.append(lead)
                    except Exception:
                        continue
                break

        return leads

    def search_all_categories(
        self, categories: list[str], limit_per_category: int = 20
    ) -> list[Lead]:
        """Busca em todas as categorias"""
        all_leads = []

        for category in categories:
            logger.info(f"Buscando: {category}")

            query = SearchQuery(
                query=category,
                location=SEARCH_LOCATION,
                category=category,
                limit=limit_per_category,
            )

            result = self.search(query)
            all_leads.extend(result.leads[:limit_per_category])

            # Delay entre categorias para evitar bloqueio
            time.sleep(DELAY_BETWEEN_REQUESTS * 2)

        return all_leads
