"""
Sistema de cache para evitar duplicatas entre execucoes

Armazena leads ja processados para:
- Evitar gastar creditos da API em leads repetidos
- Permitir atualizacao incremental
- Manter historico de leads
"""
import json
import os
import hashlib
import structlog
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

from src.models import Lead

logger = structlog.get_logger()


class LeadCache:
    """
    Cache de leads processados

    Armazena em arquivo JSON local para simplicidade.
    Para producao, considerar usar Redis ou banco de dados.
    """

    def __init__(self, cache_file: str = "data/lead_cache.json"):
        self.cache_file = Path(cache_file)
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        self._cache = self._load_cache()

    def _load_cache(self) -> dict:
        """Carrega cache do arquivo"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    logger.info(f"Cache carregado: {len(data.get('leads', {}))} leads")
                    return data
            except Exception as e:
                logger.warning(f"Erro ao carregar cache: {e}")

        return {
            "leads": {},
            "last_updated": None,
            "stats": {
                "total_processed": 0,
                "duplicates_skipped": 0,
            }
        }

    def _save_cache(self):
        """Salva cache no arquivo"""
        try:
            self._cache["last_updated"] = datetime.now().isoformat()
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erro ao salvar cache: {e}")

    def _generate_key(self, lead: Lead) -> str:
        """Gera chave unica para o lead baseada em nome + cidade"""
        # Normalizar nome (lowercase, sem espacos extras)
        nome = lead.nome.lower().strip()
        cidade = (lead.cidade or "").lower().strip()

        # Criar hash para chave curta
        key_string = f"{nome}|{cidade}"
        return hashlib.md5(key_string.encode()).hexdigest()[:16]

    def exists(self, lead: Lead) -> bool:
        """Verifica se lead ja existe no cache"""
        key = self._generate_key(lead)
        return key in self._cache["leads"]

    def get(self, lead: Lead) -> Optional[dict]:
        """Retorna dados do lead do cache"""
        key = self._generate_key(lead)
        return self._cache["leads"].get(key)

    def add(self, lead: Lead):
        """Adiciona lead ao cache"""
        key = self._generate_key(lead)

        self._cache["leads"][key] = {
            "nome": lead.nome,
            "categoria": lead.categoria,
            "cidade": lead.cidade,
            "telefone": lead.telefone,
            "email": lead.email,
            "site": lead.site,
            "instagram": lead.social.instagram if lead.social else None,
            "linkedin": lead.social.linkedin if lead.social else None,
            "score": lead.score,
            "classificacao": lead.classificacao,
            "added_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }

        self._cache["stats"]["total_processed"] += 1
        self._save_cache()

    def add_many(self, leads: list[Lead]):
        """Adiciona multiplos leads ao cache (mais eficiente)"""
        for lead in leads:
            key = self._generate_key(lead)

            self._cache["leads"][key] = {
                "nome": lead.nome,
                "categoria": lead.categoria,
                "cidade": lead.cidade,
                "telefone": lead.telefone,
                "email": lead.email,
                "site": lead.site,
                "instagram": lead.social.instagram if lead.social else None,
                "linkedin": lead.social.linkedin if lead.social else None,
                "score": lead.score,
                "classificacao": lead.classificacao,
                "added_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }

            self._cache["stats"]["total_processed"] += 1

        self._save_cache()
        logger.info(f"Adicionados {len(leads)} leads ao cache")

    def filter_new(self, leads: list[Lead]) -> list[Lead]:
        """
        Filtra apenas leads novos (que nao estao no cache)

        Returns:
            Lista de leads que ainda nao foram processados
        """
        new_leads = []
        duplicates = 0

        for lead in leads:
            if not self.exists(lead):
                new_leads.append(lead)
            else:
                duplicates += 1

        self._cache["stats"]["duplicates_skipped"] += duplicates

        if duplicates > 0:
            logger.info(
                f"Cache: {duplicates} duplicatas ignoradas, "
                f"{len(new_leads)} novos leads"
            )

        return new_leads

    def get_stats(self) -> dict:
        """Retorna estatisticas do cache"""
        return {
            "total_cached": len(self._cache["leads"]),
            "total_processed": self._cache["stats"]["total_processed"],
            "duplicates_skipped": self._cache["stats"]["duplicates_skipped"],
            "last_updated": self._cache["last_updated"],
        }

    def clear(self):
        """Limpa todo o cache"""
        self._cache = {
            "leads": {},
            "last_updated": None,
            "stats": {
                "total_processed": 0,
                "duplicates_skipped": 0,
            }
        }
        self._save_cache()
        logger.info("Cache limpo")

    def clear_old(self, days: int = 30):
        """Remove leads mais antigos que X dias"""
        cutoff = datetime.now() - timedelta(days=days)
        removed = 0

        keys_to_remove = []
        for key, data in self._cache["leads"].items():
            added_at = datetime.fromisoformat(data.get("added_at", "2000-01-01"))
            if added_at < cutoff:
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self._cache["leads"][key]
            removed += 1

        if removed > 0:
            self._save_cache()
            logger.info(f"Removidos {removed} leads antigos do cache")

        return removed

    def export_to_csv(self, filepath: str):
        """Exporta cache para CSV"""
        import csv

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # Header
            writer.writerow([
                "Nome", "Categoria", "Cidade", "Telefone", "Email",
                "Site", "Instagram", "LinkedIn", "Score", "Classificacao",
                "Data Captura"
            ])

            # Data
            for key, data in self._cache["leads"].items():
                writer.writerow([
                    data.get("nome", ""),
                    data.get("categoria", ""),
                    data.get("cidade", ""),
                    data.get("telefone", ""),
                    data.get("email", ""),
                    data.get("site", ""),
                    data.get("instagram", ""),
                    data.get("linkedin", ""),
                    data.get("score", ""),
                    data.get("classificacao", ""),
                    data.get("added_at", ""),
                ])

        logger.info(f"Cache exportado para {filepath}")
