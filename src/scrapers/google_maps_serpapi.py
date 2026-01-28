"""
Scraper do Google Maps usando SerpAPI (recomendado)
Mais estavel e com menos chances de bloqueio
"""
import time
import structlog
from typing import Optional
from serpapi import GoogleSearch

from config.settings import (
    SERPAPI_KEY,
    SEARCH_LOCATION,
    SEARCH_LANGUAGE,
    SEARCH_COUNTRY,
    REQUESTS_PER_MINUTE,
)
from src.models import Lead, SearchQuery, ScrapingResult, GoogleMapsData

logger = structlog.get_logger()


class GoogleMapsSerpAPI:
    """
    Scraper do Google Maps usando SerpAPI

    Vantagens:
    - API estavel e confiavel
    - Sem risco de bloqueio
    - Dados estruturados
    - Suporte a paginacao
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or SERPAPI_KEY
        if not self.api_key:
            raise ValueError("SERPAPI_KEY nao configurada")
        self.last_request_time = 0
        self.min_interval = 60 / REQUESTS_PER_MINUTE

    def _rate_limit(self):
        """Controle de rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()

    def search(self, query: SearchQuery) -> ScrapingResult:
        """
        Busca negocios no Google Maps

        Args:
            query: Parametros da busca

        Returns:
            ScrapingResult com lista de leads
        """
        start_time = time.time()
        leads = []
        errors = []

        try:
            self._rate_limit()

            search_query = f"{query.query} em {query.location}"
            logger.info("Iniciando busca SerpAPI", query=search_query)

            params = {
                "engine": "google_maps",
                "q": search_query,
                "ll": "@-19.9191382,-43.9386291,12z",  # Coordenadas BH
                "type": "search",
                "hl": SEARCH_LANGUAGE,
                "gl": SEARCH_COUNTRY,
                "api_key": self.api_key,
            }

            search = GoogleSearch(params)
            results = search.get_dict()

            local_results = results.get("local_results", [])
            logger.info(f"Encontrados {len(local_results)} resultados")

            for item in local_results[:query.limit]:
                try:
                    lead = self._parse_result(item, query.category)
                    leads.append(lead)
                except Exception as e:
                    errors.append(f"Erro ao processar item: {str(e)}")
                    logger.warning("Erro ao processar resultado", error=str(e))

        except Exception as e:
            errors.append(f"Erro na busca: {str(e)}")
            logger.error("Erro na busca SerpAPI", error=str(e))

        duration = time.time() - start_time

        return ScrapingResult(
            success=len(errors) == 0,
            leads=leads,
            errors=errors,
            total_found=len(leads),
            query=query,
            duration_seconds=duration,
        )

    def _parse_result(self, item: dict, category: str) -> Lead:
        """Converte resultado da API para modelo Lead"""

        # Extrair dados do Google Maps
        google_data = GoogleMapsData(
            place_id=item.get("place_id"),
            rating=item.get("rating"),
            num_reviews=item.get("reviews"),
            price_level=item.get("price"),
            types=item.get("types", []),
        )

        # Extrair telefone e site
        telefone = item.get("phone")
        site = item.get("website")

        # Verificar HTTPS
        site_https = site.startswith("https://") if site else False

        lead = Lead(
            nome=item.get("title", ""),
            categoria=category,
            telefone=telefone,
            endereco=item.get("address"),
            site=site,
            site_https=site_https,
            latitude=item.get("gps_coordinates", {}).get("latitude"),
            longitude=item.get("gps_coordinates", {}).get("longitude"),
            google_maps=google_data,
            fonte="serpapi_google_maps",
        )

        return lead

    def search_all_categories(
        self, categories: list[str], limit_per_category: int = 20
    ) -> list[Lead]:
        """
        Busca em todas as categorias configuradas

        Args:
            categories: Lista de tipos de negocios
            limit_per_category: Maximo de leads por categoria

        Returns:
            Lista consolidada de leads
        """
        all_leads = []

        for category in categories:
            logger.info(f"Buscando categoria: {category}")

            query = SearchQuery(
                query=category,
                location=SEARCH_LOCATION,
                category=category,
                limit=limit_per_category,
            )

            result = self.search(query)

            if result.success:
                all_leads.extend(result.leads)
                logger.info(
                    f"Categoria {category}: {len(result.leads)} leads encontrados"
                )
            else:
                logger.warning(
                    f"Erros na categoria {category}: {result.errors}"
                )

        # Remover duplicatas por nome + endereco
        unique_leads = self._deduplicate(all_leads)
        logger.info(
            f"Total: {len(all_leads)} leads, {len(unique_leads)} unicos"
        )

        return unique_leads

    def _deduplicate(self, leads: list[Lead]) -> list[Lead]:
        """Remove leads duplicados"""
        seen = set()
        unique = []

        for lead in leads:
            key = f"{lead.nome.lower()}|{lead.endereco or ''}"
            if key not in seen:
                seen.add(key)
                unique.append(lead)

        return unique
