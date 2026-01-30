from typing import List, Optional
from pydantic import BaseModel, Field

# --- Pydantic Models ---

class CandidateInfo(BaseModel):
    """Информация о кандидате, извлеченная из резюме."""
    name: str = Field(description="Имя кандидата", default="Не указано")
    current_location: str = Field(
        description="Текущий город проживания", default="Не указано"
    )
    industry_background: str = Field(
        description="Основной опыт в индустрии", default="Не указано"
    )


class ScoringBreakdown(BaseModel):
    """Детализация оценки кандидата."""
    hard_skills: str = Field(description="Оценка Hard Skills (X/35)")
    experience: str = Field(description="Оценка релевантности опыта (X/35)")
    location: str = Field(description="Оценка локации (X/20)")
    soft_skills_culture: str = Field(description="Оценка Soft Skills (X/10)")


class Scoring(BaseModel):
    """Общий балл и детализация."""
    total_score: int = Field(description="Общий балл 0-100")
    breakdown: ScoringBreakdown


class CandidateAnalysis(BaseModel):
    """Полная структура анализа кандидата."""
    candidate_info: CandidateInfo
    scoring: Scoring
    verdict: str = Field(description="Итоговый вердикт: Рекомендован, Резерв, Отказ")
    location_logic: str = Field(description="Обоснование оценки локации")
    pros: List[str] = Field(description="Список сильных сторон")
    cons: List[str] = Field(description="Список слабых сторон/рисков")
    red_flags: Optional[List[str]] = Field(
        description="Список критических недостатков", default=None
    )
    reasoning_chain: str = Field(description="Краткое обоснование")
