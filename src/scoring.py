"""
Sistema de Scoring de Leads B2B

Calcula pontuacao de 0-100 baseada em:
- Dados de contato disponiveis
- Presenca digital
- Indicadores de qualidade do negocio
- Fit com ICP (Ideal Customer Profile)
"""
import structlog
from typing import Optional
from datetime import datetime, timedelta

from config.settings import (
    SCORING_WEIGHTS,
    LEAD_CLASSIFICATION,
    PRIORITY_CATEGORIES,
    PRIORITY_BONUS,
)
from src.models import Lead, LeadClassification

logger = structlog.get_logger()


class LeadScorer:
    """
    Calcula score de qualificacao de leads

    Score total: 0-100 pontos
    - Dados de contato: 40 pontos
    - Presenca digital: 30 pontos
    - Qualidade do negocio: 30 pontos
    """

    def __init__(self, weights: Optional[dict] = None):
        self.weights = weights or SCORING_WEIGHTS

    def calculate_score(self, lead: Lead) -> Lead:
        """
        Calcula score completo do lead

        Args:
            lead: Lead para pontuar

        Returns:
            Lead com score e classificacao atualizados
        """
        score = 0
        breakdown = {}

        # 1. Dados de Contato (40 pontos)
        contact_score, contact_details = self._score_contact_data(lead)
        score += contact_score
        breakdown["contato"] = contact_details

        # 2. Presenca Digital (30 pontos)
        digital_score, digital_details = self._score_digital_presence(lead)
        score += digital_score
        breakdown["presenca_digital"] = digital_details

        # 3. Qualidade do Negocio (30 pontos)
        quality_score, quality_details = self._score_business_quality(lead)
        score += quality_score
        breakdown["qualidade"] = quality_details

        # Bonus para categorias prioritarias
        if lead.categoria.lower() in [c.lower() for c in PRIORITY_CATEGORIES]:
            score += PRIORITY_BONUS
            breakdown["bonus_categoria"] = PRIORITY_BONUS

        # Garantir que score esta entre 0-100
        score = max(0, min(100, score))

        # Classificar lead
        classification = self._classify_lead(score)

        # Atualizar lead
        lead.score = score
        lead.classificacao = classification
        lead.score_calculated = True
        lead.data_atualizacao = datetime.now()

        logger.info(
            f"Lead {lead.nome}: score={score}, "
            f"classificacao={classification.value}"
        )

        return lead

    def _score_contact_data(self, lead: Lead) -> tuple[int, dict]:
        """
        Pontua dados de contato (max 40 pontos)

        - Telefone: 10 pts
        - Email: 10 pts
        - Site: 10 pts
        - HTTPS: 5 pts
        - Site ativo: 5 pts
        """
        score = 0
        details = {}

        # Telefone
        if lead.telefone:
            score += self.weights.get("tem_telefone", 10)
            details["telefone"] = True
        else:
            details["telefone"] = False

        # Email
        if lead.email:
            score += self.weights.get("tem_email", 10)
            details["email"] = True
        else:
            details["email"] = False

        # Site
        if lead.site:
            score += self.weights.get("tem_site", 10)
            details["site"] = True

            # HTTPS
            if lead.site_https:
                score += self.weights.get("site_com_https", 5)
                details["https"] = True
            else:
                details["https"] = False

            # Site ativo
            if lead.site_ativo:
                score += self.weights.get("site_ativo", 5)
                details["site_ativo"] = True
            else:
                details["site_ativo"] = False
        else:
            details["site"] = False

        return score, details

    def _score_digital_presence(self, lead: Lead) -> tuple[int, dict]:
        """
        Pontua presenca digital (max 30 pontos)

        - Instagram: 10 pts
        - Instagram ativo: 5 pts
        - LinkedIn: 10 pts
        - LinkedIn company: 5 pts
        """
        score = 0
        details = {}

        social = lead.social

        # Instagram
        if social.instagram:
            score += self.weights.get("tem_instagram", 10)
            details["instagram"] = True

            # Verificar se esta ativo (posts recentes)
            if social.instagram_last_post:
                days_ago = (datetime.now() - social.instagram_last_post).days
                if days_ago <= 30:  # Post nos ultimos 30 dias
                    score += self.weights.get("instagram_ativo", 5)
                    details["instagram_ativo"] = True
                else:
                    details["instagram_ativo"] = False
            else:
                # Se nao sabemos, assumimos como ativo
                score += self.weights.get("instagram_ativo", 5) // 2
                details["instagram_ativo"] = "presumido"
        else:
            details["instagram"] = False

        # LinkedIn
        if social.linkedin:
            score += self.weights.get("tem_linkedin", 10)
            details["linkedin"] = True

            # Company page (mais valor que perfil pessoal)
            if social.linkedin_company_id or "company" in social.linkedin.lower():
                score += self.weights.get("linkedin_company_page", 5)
                details["linkedin_company"] = True
            else:
                details["linkedin_company"] = False
        else:
            details["linkedin"] = False

        return score, details

    def _score_business_quality(self, lead: Lead) -> tuple[int, dict]:
        """
        Pontua qualidade do negocio (max 30 pontos)

        - Rating alto (4+): 10 pts
        - Muitas reviews (50+): 10 pts
        - Horario funcionamento: 5 pts
        - Categoria relevante: 5 pts
        """
        score = 0
        details = {}

        google_data = lead.google_maps

        # Rating
        if google_data.rating:
            if google_data.rating >= 4.0:
                score += self.weights.get("rating_alto", 10)
                details["rating"] = f"{google_data.rating} (excelente)"
            elif google_data.rating >= 3.5:
                score += self.weights.get("rating_alto", 10) // 2
                details["rating"] = f"{google_data.rating} (bom)"
            else:
                details["rating"] = f"{google_data.rating} (baixo)"
        else:
            details["rating"] = "nao disponivel"

        # Quantidade de reviews
        if google_data.num_reviews:
            if google_data.num_reviews >= 50:
                score += self.weights.get("muitas_reviews", 10)
                details["reviews"] = f"{google_data.num_reviews} (popular)"
            elif google_data.num_reviews >= 20:
                score += self.weights.get("muitas_reviews", 10) // 2
                details["reviews"] = f"{google_data.num_reviews} (moderado)"
            else:
                details["reviews"] = f"{google_data.num_reviews} (poucos)"
        else:
            details["reviews"] = "nao disponivel"

        # Horario de funcionamento (indica negocio ativo)
        if google_data.hours:
            score += self.weights.get("horario_funcionamento", 5)
            details["horario"] = True
        else:
            details["horario"] = False

        # Categoria relevante para TimeLabs
        if lead.categoria.lower() in [c.lower() for c in PRIORITY_CATEGORIES]:
            score += self.weights.get("categoria_relevante", 5)
            details["categoria_fit"] = True
        else:
            details["categoria_fit"] = False

        return score, details

    def _classify_lead(self, score: int) -> LeadClassification:
        """Classifica lead baseado no score"""
        for classification, (min_score, max_score) in LEAD_CLASSIFICATION.items():
            if min_score <= score <= max_score:
                return LeadClassification(classification)

        return LeadClassification.LOW

    def score_leads(self, leads: list[Lead]) -> list[Lead]:
        """
        Calcula score para lista de leads

        Args:
            leads: Lista de leads

        Returns:
            Lista de leads com scores calculados
        """
        scored = []

        for lead in leads:
            try:
                scored_lead = self.calculate_score(lead)
                scored.append(scored_lead)
            except Exception as e:
                logger.error(f"Erro ao pontuar {lead.nome}: {e}")
                scored.append(lead)

        # Ordenar por score (maior primeiro)
        scored.sort(key=lambda x: x.score, reverse=True)

        return scored

    def get_summary(self, leads: list[Lead]) -> dict:
        """
        Gera resumo estatistico dos leads pontuados

        Returns:
            Dicionario com estatisticas
        """
        if not leads:
            return {}

        scores = [l.score for l in leads]
        classifications = {}

        for lead in leads:
            cls = lead.classificacao
            classifications[cls] = classifications.get(cls, 0) + 1

        return {
            "total": len(leads),
            "score_medio": sum(scores) / len(scores),
            "score_max": max(scores),
            "score_min": min(scores),
            "hot_leads": classifications.get(LeadClassification.HOT, 0),
            "warm_leads": classifications.get(LeadClassification.WARM, 0),
            "cold_leads": classifications.get(LeadClassification.COLD, 0),
            "low_leads": classifications.get(LeadClassification.LOW, 0),
        }
