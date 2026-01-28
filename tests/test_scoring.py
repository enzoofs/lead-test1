"""
Testes do sistema de scoring
"""
import pytest
from datetime import datetime

from src.models import Lead, SocialProfiles, GoogleMapsData, LeadClassification
from src.scoring import LeadScorer


class TestLeadScorer:
    """Testes para o LeadScorer"""

    def setup_method(self):
        """Setup para cada teste"""
        self.scorer = LeadScorer()

    def test_empty_lead_low_score(self):
        """Lead sem dados deve ter score baixo"""
        lead = Lead(
            nome="Empresa Teste",
            categoria="restaurante",
        )

        scored = self.scorer.calculate_score(lead)

        assert scored.score < 40
        assert scored.classificacao == LeadClassification.LOW

    def test_complete_lead_high_score(self):
        """Lead completo deve ter score alto"""
        lead = Lead(
            nome="Clinica Premium",
            categoria="clinica medica",  # Categoria prioritaria
            telefone="31999999999",
            email="contato@clinica.com.br",
            site="https://clinica.com.br",
            site_ativo=True,
            site_https=True,
            social=SocialProfiles(
                instagram="https://instagram.com/clinica",
                linkedin="https://linkedin.com/company/clinica",
                linkedin_company_id="clinica",
            ),
            google_maps=GoogleMapsData(
                rating=4.8,
                num_reviews=150,
                hours={"monday": "08:00-18:00"},
            ),
        )

        scored = self.scorer.calculate_score(lead)

        assert scored.score >= 80
        assert scored.classificacao == LeadClassification.HOT

    def test_partial_lead_medium_score(self):
        """Lead parcialmente completo deve ter score medio"""
        lead = Lead(
            nome="Empresa Media",
            categoria="academia",
            telefone="31988888888",
            site="http://empresa.com",
            site_ativo=True,
            social=SocialProfiles(
                instagram="https://instagram.com/empresa",
            ),
            google_maps=GoogleMapsData(
                rating=3.5,
                num_reviews=25,
            ),
        )

        scored = self.scorer.calculate_score(lead)

        assert 40 <= scored.score < 80

    def test_priority_category_bonus(self):
        """Categorias prioritarias devem receber bonus"""
        lead_priority = Lead(
            nome="Escritorio Adv",
            categoria="escritorio advocacia",
            telefone="31977777777",
        )

        lead_normal = Lead(
            nome="Loja Roupas",
            categoria="loja de roupas",
            telefone="31977777777",
        )

        scored_priority = self.scorer.calculate_score(lead_priority)
        scored_normal = self.scorer.calculate_score(lead_normal)

        # Categoria prioritaria deve ter score maior
        assert scored_priority.score > scored_normal.score

    def test_score_leads_batch(self):
        """Deve pontuar lista de leads"""
        leads = [
            Lead(nome="Lead 1", categoria="academia"),
            Lead(nome="Lead 2", categoria="pet shop", telefone="31999999999"),
            Lead(nome="Lead 3", categoria="clinica medica", site="https://site.com"),
        ]

        scored = self.scorer.score_leads(leads)

        assert len(scored) == 3
        # Deve estar ordenado por score (maior primeiro)
        assert scored[0].score >= scored[1].score >= scored[2].score

    def test_get_summary(self):
        """Deve gerar resumo correto"""
        leads = [
            Lead(nome="Hot", categoria="clinica medica", score=85),
            Lead(nome="Warm", categoria="academia", score=65),
            Lead(nome="Cold", categoria="pet shop", score=45),
        ]

        # Atribuir classificacoes
        leads[0].classificacao = LeadClassification.HOT
        leads[1].classificacao = LeadClassification.WARM
        leads[2].classificacao = LeadClassification.COLD

        summary = self.scorer.get_summary(leads)

        assert summary["total"] == 3
        assert summary["hot_leads"] == 1
        assert summary["warm_leads"] == 1
        assert summary["cold_leads"] == 1
        assert summary["score_medio"] == pytest.approx(65.0)


class TestLeadClassification:
    """Testes para classificacao de leads"""

    def test_hot_classification(self):
        """Score 80+ deve ser Hot"""
        scorer = LeadScorer()

        for score in [80, 90, 100]:
            classification = scorer._classify_lead(score)
            assert classification == LeadClassification.HOT

    def test_warm_classification(self):
        """Score 60-79 deve ser Warm"""
        scorer = LeadScorer()

        for score in [60, 70, 79]:
            classification = scorer._classify_lead(score)
            assert classification == LeadClassification.WARM

    def test_cold_classification(self):
        """Score 40-59 deve ser Cold"""
        scorer = LeadScorer()

        for score in [40, 50, 59]:
            classification = scorer._classify_lead(score)
            assert classification == LeadClassification.COLD

    def test_low_classification(self):
        """Score 0-39 deve ser Low"""
        scorer = LeadScorer()

        for score in [0, 20, 39]:
            classification = scorer._classify_lead(score)
            assert classification == LeadClassification.LOW
