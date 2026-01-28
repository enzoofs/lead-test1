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
import json
import structlog
from datetime import datetime
from typing import Optional
from pathlib import Path

from config.settings import BUSINESS_TYPES
from src.models import Lead, ScrapingResult
from src.scrapers import GoogleMapsSerpAPI, GoogleMapsScraper
from src.enrichers import SocialMediaExtractor, WebsiteAnalyzer, HunterEnricher
from src.scoring import LeadScorer
from src.integrations import AirtableSync
from src.cache import LeadCache

logger = structlog.get_logger()

CHECKPOINT_FILE = "data/checkpoint.json"


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
        use_variations: bool = True,
        max_neighborhoods: int = 5,
    ):
        """
        Inicializa pipeline

        Args:
            use_serpapi: Usar SerpAPI (recomendado) ou scraping direto
            use_hunter: Usar Hunter.io para enriquecimento
            sync_to_airtable: Sincronizar resultados com Airtable
            use_cache: Usar cache para evitar duplicatas entre execucoes
            use_variations: Usar bairros e sinonimos na busca
            max_neighborhoods: Maximo de bairros por categoria
        """
        self.use_serpapi = use_serpapi
        self.use_hunter = use_hunter
        self.sync_to_airtable = sync_to_airtable
        self.use_cache = use_cache
        self.use_variations = use_variations
        self.max_neighborhoods = max_neighborhoods

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

    def _save_checkpoint(self, stage: int, leads: list[Lead], results: dict):
        """Salva checkpoint para poder retomar execucao"""
        checkpoint_path = Path(CHECKPOINT_FILE)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        leads_data = [lead.model_dump(mode="json") for lead in leads]

        checkpoint = {
            "stage": stage,
            "leads": leads_data,
            "results": results,
            "saved_at": datetime.now().isoformat(),
        }

        with open(checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Checkpoint salvo: stage {stage}, {len(leads)} leads")

    def _load_checkpoint(self) -> Optional[dict]:
        """Carrega checkpoint anterior se existir"""
        checkpoint_path = Path(CHECKPOINT_FILE)
        if not checkpoint_path.exists():
            return None

        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Reconstituir leads
            leads = [Lead.model_validate(ld) for ld in data.get("leads", [])]
            data["leads"] = leads

            logger.info(
                f"Checkpoint encontrado: stage {data['stage']}, "
                f"{len(leads)} leads, salvo em {data['saved_at']}"
            )
            return data
        except Exception as e:
            logger.warning(f"Erro ao carregar checkpoint: {e}")
            return None

    def _clear_checkpoint(self):
        """Remove checkpoint apos execucao completa"""
        checkpoint_path = Path(CHECKPOINT_FILE)
        if checkpoint_path.exists():
            checkpoint_path.unlink()
            logger.info("Checkpoint removido (execucao completa)")

    def run(
        self,
        categories: Optional[list[str]] = None,
        limit_per_category: int = 20,
        resume: bool = False,
    ) -> dict:
        """
        Executa pipeline completo

        Args:
            categories: Categorias para buscar (default: todas)
            limit_per_category: Max leads por categoria
            resume: Retomar de checkpoint anterior

        Returns:
            Resumo da execucao
        """
        start_time = time.time()
        categories = categories or BUSINESS_TYPES
        resume_stage = 0
        leads = []

        # Tentar retomar de checkpoint
        if resume:
            checkpoint = self._load_checkpoint()
            if checkpoint:
                resume_stage = checkpoint["stage"]
                leads = checkpoint["leads"]
                results = checkpoint.get("results", {
                    "started_at": datetime.now().isoformat(),
                    "categories": categories,
                    "stages": {},
                })
                logger.info(
                    f"Retomando pipeline do stage {resume_stage + 1} "
                    f"com {len(leads)} leads"
                )
            else:
                logger.info("Nenhum checkpoint encontrado, iniciando do zero")
                resume = False

        if not resume:
            results = {
                "started_at": datetime.now().isoformat(),
                "categories": categories,
                "stages": {},
            }

        logger.info(
            f"Iniciando pipeline: {len(categories)} categorias, "
            f"limit={limit_per_category}"
        )

        # Testar conexao Airtable ANTES de gastar API calls
        if self.airtable:
            logger.info("Testando conexao com Airtable...")
            if not self.airtable.test_connection():
                logger.error(
                    "ABORTANDO: Airtable sem permissao. "
                    "Corrija as permissoes do token antes de executar. "
                    "Use --no-airtable para rodar sem sincronizar."
                )
                results["error"] = "Airtable permission denied (403)"
                return results

        # Stage 1: Scraping
        if resume_stage < 1:
            logger.info("=== Stage 1: Scraping Google Maps ===")
            leads = self.scraper.search_all_categories(
                categories, limit_per_category,
                use_variations=self.use_variations,
                max_neighborhoods=self.max_neighborhoods,
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

            self._save_checkpoint(1, leads, results)

        # Stage 2: Analise de websites
        if resume_stage < 2:
            logger.info("=== Stage 2: Analise de Websites ===")
            leads = self.website_analyzer.analyze_leads(leads)
            sites_ativos = sum(1 for l in leads if l.site_ativo)
            results["stages"]["website_analysis"] = {
                "sites_ativos": sites_ativos,
                "sites_https": sum(1 for l in leads if l.site_https),
            }
            logger.info(f"Websites: {sites_ativos} sites ativos")
            self._save_checkpoint(2, leads, results)

        # Stage 3: Extracao de redes sociais, emails e telefones
        if resume_stage < 3:
            logger.info("=== Stage 3: Extracao de Redes Sociais, Emails e Telefones ===")
            leads = self.social_extractor.enrich_leads(leads)

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
            self._save_checkpoint(3, leads, results)

        # Stage 4: Enriquecimento Hunter.io (opcional)
        if resume_stage < 4:
            if self.hunter:
                logger.info("=== Stage 4: Enriquecimento Hunter.io ===")
                leads = self.hunter.enrich_leads(leads)
                emails_found = sum(1 for l in leads if l.email)
                results["stages"]["hunter_enrichment"] = {
                    "emails_found": emails_found,
                }
                logger.info(f"Hunter.io: {emails_found} emails encontrados")
            self._save_checkpoint(4, leads, results)

        # Stage 5: Scoring
        if resume_stage < 5:
            logger.info("=== Stage 5: Scoring ===")
            leads = self.scorer.score_leads(leads)
            summary = self.scorer.get_summary(leads)
            results["stages"]["scoring"] = summary
            logger.info(
                f"Scoring: score medio={summary['score_medio']:.1f}, "
                f"hot={summary['hot_leads']}, warm={summary['warm_leads']}"
            )
            self._save_checkpoint(5, leads, results)

        # Stage 6: Sincronizacao Airtable
        if self.airtable:
            logger.info("=== Stage 6: Sincronizacao Airtable ===")
            sync_result = self.airtable.sync_leads(leads)
            results["stages"]["airtable_sync"] = sync_result
            logger.info(
                f"Airtable: {sync_result['created']} criados, "
                f"{sync_result['updated']} atualizados"
            )

        # Salvar leads no cache - SOMENTE leads confirmados no Airtable
        # Se Airtable esta ativo, so cacheia leads que foram sincronizados com sucesso
        # Se Airtable esta desativado, cacheia todos (comportamento anterior)
        if self.cache:
            if self.airtable:
                synced_leads = [l for l in leads if l.synced_to_airtable]
                if synced_leads:
                    self.cache.add_many(synced_leads)
                    logger.info(
                        f"Cache atualizado: {len(synced_leads)} leads sincronizados "
                        f"(total cache: {self.cache.get_stats()['total_cached']})"
                    )
                failed_leads = [l for l in leads if not l.synced_to_airtable]
                if failed_leads:
                    logger.warning(
                        f"{len(failed_leads)} leads NAO cacheados "
                        f"(falha no Airtable - serao reprocessados na proxima execucao)"
                    )
            else:
                self.cache.add_many(leads)
                logger.info(f"Cache atualizado: {self.cache.get_stats()['total_cached']} leads")

        # Finalizar
        duration = time.time() - start_time
        results["duration_seconds"] = duration
        results["finished_at"] = datetime.now().isoformat()
        results["total_leads"] = len(leads)

        # Limpar checkpoint apos execucao completa com sucesso
        self._clear_checkpoint()

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
