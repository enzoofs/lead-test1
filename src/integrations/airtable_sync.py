"""
Sincronizacao com Airtable

Gerencia a persistencia de leads no Airtable:
- Cria novos registros
- Atualiza existentes
- Evita duplicatas
"""
import structlog
from datetime import datetime
from typing import Optional
from pyairtable import Api, Table
from pyairtable.formulas import match

from config.settings import (
    AIRTABLE_API_KEY,
    AIRTABLE_BASE_ID,
    AIRTABLE_TABLE_NAME,
)
from src.models import Lead

logger = structlog.get_logger()


class AirtableSync:
    """
    Sincroniza leads com Airtable

    Campos mapeados:
    - Nome, Categoria, Telefone, Email
    - Endereco, Cidade, Site
    - Instagram, LinkedIn
    - Rating, Reviews, Score, Classificacao
    - Status, Data Captura, Notas
    """

    FIELD_MAPPING = {
        "nome": "Nome",
        "categoria": "Categoria",
        "telefone": "Telefone",
        "email": "Email",
        "endereco": "Endereco",
        "cidade": "Cidade",
        "site": "Site",
        "instagram": "Instagram",
        "linkedin": "LinkedIn",
        "rating": "Rating",
        "num_reviews": "Num Reviews",
        "score": "Score",
        "classificacao": "Classificacao",
        "status": "Status",
        "data_captura": "Data Captura",
        "notas": "Notas",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_id: Optional[str] = None,
        table_name: Optional[str] = None,
    ):
        self.api_key = api_key or AIRTABLE_API_KEY
        self.base_id = base_id or AIRTABLE_BASE_ID
        self.table_name = table_name or AIRTABLE_TABLE_NAME

        if not all([self.api_key, self.base_id]):
            raise ValueError(
                "AIRTABLE_API_KEY e AIRTABLE_BASE_ID sao obrigatorios"
            )

        self.api = Api(self.api_key)
        self.table = self.api.table(self.base_id, self.table_name)

        logger.info(f"Airtable conectado: {self.base_id}/{self.table_name}")

    def test_connection(self) -> bool:
        """
        Testa conexao e permissoes do Airtable antes de iniciar o pipeline.
        Retorna True se tudo OK, False se ha problemas.
        """
        try:
            # Tenta listar 1 registro para verificar permissoes
            self.table.all(max_records=1)
            logger.info("Airtable: conexao e permissoes OK")
            return True
        except Exception as e:
            error_str = str(e)
            if "403" in error_str or "INVALID_PERMISSIONS" in error_str:
                logger.error(
                    "Airtable: ERRO DE PERMISSAO (403). "
                    "Verifique se o token tem as permissoes: "
                    "data.records:read, data.records:write "
                    "e se a base esta adicionada ao token. "
                    "Acesse: https://airtable.com/create/tokens"
                )
            elif "404" in error_str or "NOT_FOUND" in error_str:
                logger.error(
                    f"Airtable: Base ou tabela nao encontrada. "
                    f"Base ID: {self.base_id}, Tabela: {self.table_name}"
                )
            else:
                logger.error(f"Airtable: Erro de conexao: {e}")
            return False

    def _lead_to_record(self, lead: Lead) -> dict:
        """Converte Lead para registro Airtable"""
        record = {
            self.FIELD_MAPPING["nome"]: lead.nome,
            self.FIELD_MAPPING["categoria"]: lead.categoria,
            self.FIELD_MAPPING["cidade"]: lead.cidade,
            self.FIELD_MAPPING["score"]: lead.score,
            self.FIELD_MAPPING["classificacao"]: lead.classificacao,
            self.FIELD_MAPPING["status"]: lead.status,
            self.FIELD_MAPPING["data_captura"]: lead.data_captura.isoformat(),
        }

        # Campos opcionais
        if lead.telefone:
            record[self.FIELD_MAPPING["telefone"]] = lead.telefone

        if lead.email:
            record[self.FIELD_MAPPING["email"]] = lead.email

        if lead.endereco:
            record[self.FIELD_MAPPING["endereco"]] = lead.endereco

        if lead.site:
            record[self.FIELD_MAPPING["site"]] = lead.site

        if lead.social.instagram:
            record[self.FIELD_MAPPING["instagram"]] = lead.social.instagram

        if lead.social.linkedin:
            record[self.FIELD_MAPPING["linkedin"]] = lead.social.linkedin

        if lead.google_maps.rating:
            record[self.FIELD_MAPPING["rating"]] = lead.google_maps.rating

        if lead.google_maps.num_reviews:
            record[self.FIELD_MAPPING["num_reviews"]] = lead.google_maps.num_reviews

        if lead.notas:
            record[self.FIELD_MAPPING["notas"]] = lead.notas

        return record

    def _find_existing(self, lead: Lead) -> Optional[str]:
        """
        Procura lead existente no Airtable

        Usa nome + cidade como chave unica
        """
        try:
            formula = match({
                self.FIELD_MAPPING["nome"]: lead.nome,
                self.FIELD_MAPPING["cidade"]: lead.cidade,
            })

            records = self.table.all(formula=formula)

            if records:
                return records[0]["id"]

        except Exception as e:
            logger.warning(f"Erro ao buscar existente: {e}")

        return None

    def upsert(self, lead: Lead) -> dict:
        """
        Insere ou atualiza lead no Airtable

        Args:
            lead: Lead para sincronizar

        Returns:
            Registro criado/atualizado
        """
        record_data = self._lead_to_record(lead)

        # Verificar se ja existe
        existing_id = self._find_existing(lead)

        if existing_id:
            # Atualizar
            logger.info(f"Atualizando lead: {lead.nome}")
            result = self.table.update(existing_id, record_data)
        else:
            # Criar novo
            logger.info(f"Criando lead: {lead.nome}")
            result = self.table.create(record_data)

        lead.synced_to_airtable = True
        lead.id = result["id"]

        return result

    def sync_leads(self, leads: list[Lead]) -> dict:
        """
        Sincroniza lista de leads com Airtable

        Args:
            leads: Lista de leads

        Returns:
            Resumo da sincronizacao
        """
        created = 0
        updated = 0
        errors = []

        for i, lead in enumerate(leads, 1):
            logger.info(f"Sincronizando {i}/{len(leads)}: {lead.nome}")

            try:
                existing_id = self._find_existing(lead)

                if existing_id:
                    self.table.update(existing_id, self._lead_to_record(lead))
                    updated += 1
                else:
                    self.table.create(self._lead_to_record(lead))
                    created += 1

                lead.synced_to_airtable = True

            except Exception as e:
                error_msg = f"Erro ao sincronizar {lead.nome}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)

        summary = {
            "total": len(leads),
            "created": created,
            "updated": updated,
            "errors": len(errors),
            "error_details": errors,
        }

        logger.info(f"Sincronizacao concluida: {summary}")

        return summary

    def batch_create(self, leads: list[Lead]) -> dict:
        """
        Cria leads em batch (mais rapido)

        NOTA: Nao verifica duplicatas, usar com cuidado
        """
        records = [self._lead_to_record(lead) for lead in leads]

        # Airtable aceita max 10 registros por chamada
        created = 0
        errors = []

        for i in range(0, len(records), 10):
            batch = records[i:i+10]

            try:
                self.table.batch_create(batch)
                created += len(batch)
            except Exception as e:
                errors.append(str(e))
                logger.error(f"Erro no batch {i}: {e}")

        return {
            "created": created,
            "errors": len(errors),
        }

    def get_all_leads(self) -> list[dict]:
        """Retorna todos os leads do Airtable"""
        return self.table.all()

    def get_hot_leads(self) -> list[dict]:
        """Retorna apenas leads Hot"""
        formula = match({self.FIELD_MAPPING["classificacao"]: "hot"})
        return self.table.all(formula=formula)

    def delete_all(self) -> int:
        """
        Deleta todos os registros

        CUIDADO: Operacao destrutiva!
        """
        records = self.table.all()
        count = 0

        for record in records:
            try:
                self.table.delete(record["id"])
                count += 1
            except Exception as e:
                logger.error(f"Erro ao deletar: {e}")

        return count
