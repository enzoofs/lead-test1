"""
Pipeline completo de captacao e qualificacao de leads

Orquestra todos os modulos:
1. Scraping do Google Maps
2. Extracao de redes sociais
3. Enriquecimento (opcional)
4. Scoring
5. Sincronizacao com Airtable
"""
import time
import structlog
from datetime import datetime
from typing import Optional

from config.settings import BUSINESS_TYPES
from src.models import Lead, ScrapingResult
from src.scrapers import GoogleMapsSerpAPI, GoogleMapsScraper
from src.enrichers import SocialMediaExtractor, WebsiteAnalyzer, HunterEnricher
from src.scoring import LeadScorer
from src.integrations import AirtableSync
from src.cache import LeadCache

logger = structlog.get_logger()


class LeadPipeline:
    """
    Pipeline de captacao e qualificacao de leads

    Fluxo:
    1. Scraping Google Maps -> lista de leads basicos
    2. Analise de websites -> verifica sites ativos
    3. Extracao de redes sociais -> Instagram, LinkedIn
    4. Enriquecimento Hunter.io -> emails, dados extras
    5. Scoring -> pontuacao e classificacao
    6. Sincronizacao Airtable -> persistencia
    """

    def __init__(
        self,
        use_serpapi: bool = True,
        use_hunter: bool = False,
        sync_to_airtable: bool = True,
        use_cache: bool = True,
    ):
        """
        Inicializa pipeline

        Args:
            use_serpapi: Usar SerpAPI (recomendado) ou scraping direto
            use_hunter: Usar Hunter.io para enriquecimento
            sync_to_airtable: Sincronizar resultados com Airtable
            use_cache: Usar cache para evitar duplicatas entre execucoes
        """
        self.use_serpapi = use_serpapi
        self.use_hunter = use_hunter
        self.sync_to_airtable = sync_to_airtable
        self.use_cache = use_cache

        # Inicializar componentes
        self._init_components()

    def _init_components(self):
        """Inicializa componentes do pipeline"""
        # Scraper
        if self.use_serpapi:
            try:
                self.scraper = GoogleMapsSerpAPI()
                logger.info("Usando SerpAPI para scraping")
            except ValueError:
                logger.warning("SerpAPI nao configurada, usando scraping direto")
                self.scraper = GoogleMapsScraper()
        else:
            self.scraper = GoogleMapsScraper()
            logger.info("Usando scraping direto")

        # Enrichers
        self.website_analyzer = WebsiteAnalyzer()
        self.social_extractor = SocialMediaExtractor()

        if self.use_hunter:
            try:
                self.hunter = HunterEnricher()
                logger.info("Hunter.io ativado")
            except Exception:
                self.hunter = None
                logger.warning("Hunter.io nao disponivel")
        else:
            self.hunter = None

        # Scorer
        self.scorer = LeadScorer()

        # Airtable
        if self.sync_to_airtable:
            try:
                self.airtable = AirtableSync()
                logger.info("Airtable conectado")
            except Exception as e:
                self.airtable = None
                logger.warning(f"Airtable nao disponivel: {e}")
        else:
            self.airtable = None

        # Cache
        if self.use_cache:
            self.cache = LeadCache()
            stats = self.cache.get_stats()
            logger.info(f"Cache ativo: {stats['total_cached']} leads em cache")
        else:
            self.cache = None

    def run(
        self,
        categories: Optional[list[str]] = None,
        limit_per_category: int = 20,
    ) -> dict:
        """
        Executa pipeline completo

        Args:
            categories: Categorias para buscar (default: todas)
            limit_per_category: Max leads por categoria

        Returns:
            Resumo da execucao
        """
        start_time = time.time()
        categories = categories or BUSINESS_TYPES

        logger.info(
            f"Iniciando pipeline: {len(categories)} categorias, "
            f"limit={limit_per_category}"
        )

        results = {
            "started_at": datetime.now().isoformat(),
            "categories": categories,
            "stages": {},
        }

        # Stage 1: Scraping
        logger.info("=== Stage 1: Scraping Google Maps ===")
        leads = self.scraper.search_all_categories(
            categories, limit_per_category
        )
        total_scraped = len(leads)
        results["stages"]["scraping"] = {
            "leads_found": total_scraped,
        }
        logger.info(f"Scraping: {total_scraped} leads encontrados")

        if not leads:
            logger.warning("Nenhum lead encontrado, encerrando")
            return results

        # Filtrar duplicatas usando cache
        if self.cache:
            leads = self.cache.filter_new(leads)
            results["stages"]["scraping"]["new_leads"] = len(leads)
            results["stages"]["scraping"]["cached_skipped"] = total_scraped - len(leads)

            if not leads:
                logger.info("Todos os leads ja estao em cache")
                results["total_leads"] = 0
                return results

        # Stage 2: Analise de websites
        logger.info("=== Stage 2: Analise de Websites ===")
        leads = self.website_analyzer.analyze_leads(leads)
        sites_ativos = sum(1 for l in leads if l.site_ativo)
        results["stages"]["website_analysis"] = {
            "sites_ativos": sites_ativos,
            "sites_https": sum(1 for l in leads if l.site_https),
        }
        logger.info(f"Websites: {sites_ativos} sites ativos")

        # Stage 3: Extracao de redes sociais, emails e telefones
        logger.info("=== Stage 3: Extracao de Redes Sociais, Emails e Telefones ===")
        leads = self.social_extractor.enrich_leads(leads)

        # Contar resultados
        instagram_count = sum(1 for l in leads if l.social.instagram)
        linkedin_count = sum(1 for l in leads if l.social.linkedin)
        email_count = sum(1 for l in leads if l.email)
        telefone_count = sum(1 for l in leads if l.telefone)

        results["stages"]["social_extraction"] = {
            "instagram": instagram_count,
            "linkedin": linkedin_count,
            "emails": email_count,
            "telefones": telefone_count,
        }
        logger.info(
            f"Redes sociais: {instagram_count} Instagram, "
            f"{linkedin_count} LinkedIn"
        )
        logger.info(
            f"Contato: {email_count} emails, {telefone_count} telefones"
        )

        # Stage 4: Enriquecimento Hunter.io (opcional)
        if self.hunter:
            logger.info("=== Stage 4: Enriquecimento Hunter.io ===")
            leads = self.hunter.enrich_leads(leads)
            emails_found = sum(1 for l in leads if l.email)
            results["stages"]["hunter_enrichment"] = {
                "emails_found": emails_found,
            }
            logger.info(f"Hunter.io: {emails_found} emails encontrados")

        # Stage 5: Scoring
        logger.info("=== Stage 5: Scoring ===")
        leads = self.scorer.score_leads(leads)
        summary = self.scorer.get_summary(leads)
        results["stages"]["scoring"] = summary
        logger.info(
            f"Scoring: score medio={summary['score_medio']:.1f}, "
            f"hot={summary['hot_leads']}, warm={summary['warm_leads']}"
        )

        # Stage 6: Sincronizacao Airtable
        if self.airtable:
            logger.info("=== Stage 6: Sincronizacao Airtable ===")
            sync_result = self.airtable.sync_leads(leads)
            results["stages"]["airtable_sync"] = sync_result
            logger.info(
                f"Airtable: {sync_result['created']} criados, "
                f"{sync_result['updated']} atualizados"
            )

        # Salvar leads no cache
        if self.cache:
            self.cache.add_many(leads)
            logger.info(f"Cache atualizado: {self.cache.get_stats()['total_cached']} leads")

        # Finalizar
        duration = time.time() - start_time
        results["duration_seconds"] = duration
        results["finished_at"] = datetime.now().isoformat()
        results["total_leads"] = len(leads)

        logger.info(f"Pipeline concluido em {duration:.1f}s")
        logger.info(f"Total: {len(leads)} leads processados")

        return results

    def run_single_category(
        self, category: str, limit: int = 20
    ) -> list[Lead]:
        """
        Processa uma unica categoria

        Util para testes e debugging
        """
        return self.run(categories=[category], limit_per_category=limit)

    def export_to_csv(self, leads: list[Lead], filepath: str):
        """Exporta leads para CSV"""
        import csv

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # Header
            writer.writerow([
                "Nome", "Categoria", "Telefone", "Email",
                "Endereco", "Site", "Instagram", "LinkedIn",
                "Rating", "Reviews", "Score", "Classificacao"
            ])

            # Data
            for lead in leads:
                writer.writerow([
                    lead.nome,
                    lead.categoria,
                    lead.telefone or "",
                    lead.email or "",
                    lead.endereco or "",
                    lead.site or "",
                    lead.social.instagram or "",
                    lead.social.linkedin or "",
                    lead.google_maps.rating or "",
                    lead.google_maps.num_reviews or "",
                    lead.score,
                    lead.classificacao,
                ])

        logger.info(f"Exportado para {filepath}")


def run_daily_pipeline():
    """
    Funcao para execucao diaria do pipeline

    Pode ser chamada por cron ou N8N
    """
    pipeline = LeadPipeline(
        use_serpapi=True,
        use_hunter=False,  # Economizar creditos
        sync_to_airtable=True,
    )

    results = pipeline.run(limit_per_category=20)

    return results


if __name__ == "__main__":
    # Execucao de teste
    import json

    pipeline = LeadPipeline(
        use_serpapi=True,
        use_hunter=False,
        sync_to_airtable=False,  # Desativar para teste
    )

    # Testar com uma categoria
    results = pipeline.run(
        categories=["clinica medica"],
        limit_per_category=5,
    )

    print(json.dumps(results, indent=2, default=str))
