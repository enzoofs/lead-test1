#!/usr/bin/env python3
"""
Sistema de Captacao e Qualificacao de Leads B2B - TimeLabs

Uso:
    python main.py                    # Executa pipeline completo
    python main.py --category "clinica medica"  # Uma categoria
    python main.py --test             # Modo teste (5 leads, sem Airtable)
    python main.py --export leads.csv # Exporta para CSV
"""
import argparse
import json
import sys
import structlog
from datetime import datetime

from config.settings import BUSINESS_TYPES
from src.pipeline import LeadPipeline

# Configurar logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(colors=True),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()


def main():
    parser = argparse.ArgumentParser(
        description="Sistema de Captacao de Leads B2B - TimeLabs"
    )

    parser.add_argument(
        "--category", "-c",
        type=str,
        help="Categoria especifica para buscar",
    )

    parser.add_argument(
        "--categories",
        type=str,
        nargs="+",
        help="Lista de categorias para buscar",
    )

    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=20,
        help="Limite de leads por categoria (default: 20)",
    )

    parser.add_argument(
        "--test", "-t",
        action="store_true",
        help="Modo teste: 5 leads, sem Airtable",
    )

    parser.add_argument(
        "--no-serpapi",
        action="store_true",
        help="Usar scraping direto em vez de SerpAPI",
    )

    parser.add_argument(
        "--hunter",
        action="store_true",
        help="Ativar enriquecimento Hunter.io",
    )

    parser.add_argument(
        "--no-airtable",
        action="store_true",
        help="Nao sincronizar com Airtable",
    )

    parser.add_argument(
        "--export", "-e",
        type=str,
        help="Exportar resultados para CSV",
    )

    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Salvar resultados JSON em arquivo",
    )

    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="Listar categorias disponiveis",
    )

    args = parser.parse_args()

    # Listar categorias
    if args.list_categories:
        print("\nCategorias disponiveis:")
        for i, cat in enumerate(BUSINESS_TYPES, 1):
            print(f"  {i}. {cat}")
        return

    # Determinar categorias
    if args.category:
        categories = [args.category]
    elif args.categories:
        categories = args.categories
    else:
        categories = BUSINESS_TYPES

    # Configurar pipeline
    limit = 5 if args.test else args.limit
    sync_airtable = not (args.test or args.no_airtable)

    logger.info("=" * 60)
    logger.info("Sistema de Captacao de Leads B2B - TimeLabs")
    logger.info("=" * 60)
    logger.info(f"Categorias: {len(categories)}")
    logger.info(f"Limite por categoria: {limit}")
    logger.info(f"SerpAPI: {not args.no_serpapi}")
    logger.info(f"Hunter.io: {args.hunter}")
    logger.info(f"Airtable: {sync_airtable}")
    logger.info("=" * 60)

    # Criar e executar pipeline
    pipeline = LeadPipeline(
        use_serpapi=not args.no_serpapi,
        use_hunter=args.hunter,
        sync_to_airtable=sync_airtable,
    )

    try:
        results = pipeline.run(
            categories=categories,
            limit_per_category=limit,
        )

        # Exibir resumo
        print("\n" + "=" * 60)
        print("RESUMO DA EXECUCAO")
        print("=" * 60)
        print(f"Total de leads: {results.get('total_leads', 0)}")
        print(f"Duracao: {results.get('duration_seconds', 0):.1f}s")

        if "stages" in results:
            stages = results["stages"]

            if "scoring" in stages:
                scoring = stages["scoring"]
                print(f"\nDistribuicao de leads:")
                print(f"  Hot:  {scoring.get('hot_leads', 0)}")
                print(f"  Warm: {scoring.get('warm_leads', 0)}")
                print(f"  Cold: {scoring.get('cold_leads', 0)}")
                print(f"  Low:  {scoring.get('low_leads', 0)}")
                print(f"  Score medio: {scoring.get('score_medio', 0):.1f}")

            if "social_extraction" in stages:
                social = stages["social_extraction"]
                print(f"\nRedes sociais encontradas:")
                print(f"  Instagram: {social.get('instagram', 0)}")
                print(f"  LinkedIn:  {social.get('linkedin', 0)}")

        # Exportar CSV
        if args.export:
            # Precisamos dos leads para exportar
            # Por enquanto, mostrar mensagem
            print(f"\nExportacao CSV: {args.export}")
            print("(Implementar: salvar leads durante pipeline)")

        # Salvar JSON
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\nResultados salvos em: {args.output}")

        print("=" * 60)

    except KeyboardInterrupt:
        logger.warning("Execucao interrompida pelo usuario")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        raise


if __name__ == "__main__":
    main()
