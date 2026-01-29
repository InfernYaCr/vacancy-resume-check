import os
import json
import time
import traceback
import logging
from typing import List, Optional, Dict, Any, Tuple
from openai import OpenAI, RateLimitError, APIError
from pydantic import BaseModel, Field, ValidationError

from config import config
from extract_mhtml import extract_text_from_mhtml
from generate_report import generate_markdown_report

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# --- Pydantic модели ---
class CandidateInfo(BaseModel):
    name: str = Field(description="Имя кандидата", default="Не указано")
    current_location: str = Field(
        description="Текущий город проживания", default="Не указано"
    )
    industry_background: str = Field(
        description="Основной опыт в индустрии", default="Не указано"
    )


class ScoringBreakdown(BaseModel):
    hard_skills: str = Field(description="Оценка Hard Skills (X/35)")
    experience: str = Field(description="Оценка релевантности опыта (X/35)")
    location: str = Field(description="Оценка локации (X/20)")
    soft_skills_culture: str = Field(description="Оценка Soft Skills (X/10)")


class Scoring(BaseModel):
    total_score: int = Field(description="Общий балл 0-100")
    breakdown: ScoringBreakdown


class CandidateAnalysis(BaseModel):
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


# Инициализация LLM клиента
client = OpenAI(api_key=config.LLM_API_KEY, base_url=config.LLM_BASE_URL)


def smart_sleep(seconds: float) -> None:
    """Обертка для sleep, чтобы было проще мокать или улучшать."""
    logger.info(f"    Ожидание {seconds} сек для соблюдения лимитов API...")
    time.sleep(seconds)


def get_llm_analysis(
    resume_text: str, vacancy_text: str, prompt_template_name: str = "hr_expert_v2.txt"
) -> Optional[Dict[str, Any]]:
    """
    Отправляет запрос в LLM и возвращает разобранный JSON, валидированный через Pydantic.
    Обрабатывает лимиты запросов (429) с экспоненциальной задержкой.
    """
    max_retries = 5
    base_delay = 5

    try:
        prompt_template = config.load_prompt(prompt_template_name)

        # Обрезаем текст, чтобы избежать лимитов токенов (базовая защита)
        final_prompt = prompt_template.replace(
            "{resume_text}", resume_text[:20000]
        ).replace("{vacancy_text}", vacancy_text[:10000])

        logger.debug(
            f"Промпт подготовлен. Длина резюме: {len(resume_text)}, Длина вакансии: {len(vacancy_text)}"
        )

        for attempt in range(max_retries):
            try:
                logger.info(
                    f"    Попытка {attempt+1}/{max_retries}. Отправка запроса к {config.LLM_MODEL}..."
                )

                response = client.chat.completions.create(
                    model=config.LLM_MODEL,
                    temperature=config.LLM_TEMPERATURE,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a precise JSON-outputting engine. Output ONLY valid JSON matching the schema.",
                        },
                        {"role": "user", "content": final_prompt},
                    ],
                    response_format={"type": "json_object"},
                )

                content = response.choices[0].message.content
                if not content:
                    logger.warning("Получен пустой ответ от LLM")
                    return None

                # Очистка markdown обертки, если есть
                if content.startswith("```"):
                    content = content.strip("`").strip()
                    if content.startswith("json"):
                        content = content[4:].strip()

                try:
                    # Строгая валидация Pydantic
                    analysis_data = CandidateAnalysis.model_validate_json(content)
                    return analysis_data.model_dump()
                except ValidationError as e:
                    logger.error(f"Ошибка валидации Pydantic:\n{e}")
                    return None
                except json.JSONDecodeError:
                    logger.error(
                        f"Ошибка парсинга JSON. Фрагмент ответа LLM:\n{content[:200]}..."
                    )
                    return None

            except RateLimitError:
                wait_time = base_delay * (2**attempt)
                logger.warning(f"Превышен лимит (429). Ожидание {wait_time} сек...")
                time.sleep(wait_time)
            except APIError as e:
                # Специальная обработка для OpenRouter 429
                if getattr(e, "code", None) == 429:
                    wait_time = base_delay * (2**attempt)
                    logger.warning(f"APIError 429. Ожидание {wait_time} сек...")
                    time.sleep(wait_time)
                else:
                    raise e

        logger.error("Не удалось получить ответ после всех попыток")
        return None

    except Exception as e:
        logger.error(f"Ошибка взаимодействия с LLM: {e}", exc_info=True)
        return None


def get_candidate_files(work_dir: str) -> Tuple[List[str], List[str]]:
    """Сканирует директорию и разделяет вакансии и резюме по ключевым словам."""
    if not os.path.exists(work_dir):
        logger.error(f"Директория {work_dir} не найдена.")
        return [], []

    files = [f for f in os.listdir(work_dir) if f.lower().endswith(".mhtml")]
    vacancies = []
    resumes = []

    for f in files:
        full_path = os.path.join(work_dir, f)
        # Улучшение: В реальной системе стоит проверять контент, но пока следуем архитектуре
        if config.VACANCY_KEYWORD.lower() in f.lower():
            vacancies.append(full_path)
        else:
            resumes.append(full_path)

    logger.info(f"Найдено вакансий: {len(vacancies)}, резюме: {len(resumes)}.")
    return vacancies, resumes


def process_batch(vacancies: List[str], resumes: List[str]) -> List[Dict[str, Any]]:
    """Запускает анализ для всех комбинаций вакансий и резюме."""
    results = []

    for vacancy_path in vacancies:
        logger.info(f"\n--- Анализ вакансии: {os.path.basename(vacancy_path)} ---")
        vacancy_text = extract_text_from_mhtml(vacancy_path)

        if not vacancy_text:
            logger.error("Не удалось извлечь текст из вакансии.")
            continue

        for resume_path in resumes:
            logger.info(f"  > Обработка кандидата: {os.path.basename(resume_path)}...")
            resume_text = extract_text_from_mhtml(resume_path)

            if not resume_text:
                logger.error("    Не удалось извлечь текст из резюме.")
                continue

            analysis = get_llm_analysis(resume_text, vacancy_text)

            if analysis:
                # Обогащение метаданными
                analysis["vacancy_file"] = os.path.basename(vacancy_path)
                analysis["resume_file"] = os.path.basename(resume_path)
                results.append(analysis)

                score = analysis.get("scoring", {}).get("total_score", "N/A")
                logger.info(f"    Анализ завершен. Оценка: {score}")
            else:
                logger.warning("    Анализ не удался (LLM вернула None).")

            smart_sleep(5)  # Задержка

    return results


def save_results(results: List[Dict[str, Any]], reports_dir: str = "reports") -> None:
    """Сохраняет сырые JSON результаты и генерирует Markdown отчет."""
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(reports_dir, f"analysis_results_{timestamp}.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    logger.info(f"\nГотово! Сырые результаты сохранены в {output_file}")

    logger.info("Генерация Markdown отчета...")
    report_content = generate_markdown_report(results)
    if report_content:
        report_filename = os.path.join(reports_dir, f"report_{timestamp}.md")
        with open(report_filename, "w", encoding="utf-8") as f:
            f.write(report_content)
        logger.info(f"Отчет успешно создан: {report_filename}")


def main():
    work_dir = "resume vs vacancy"

    vacancies, resumes = get_candidate_files(work_dir)
    if not vacancies or not resumes:
        logger.warning(
            "Недостаточно файлов для обработки. Проверьте папку 'resume vs vacancy'."
        )
        return

    # Опционально: Раскомментируйте для теста
    # vacancies = vacancies[:1]
    # resumes = resumes[:1]

    results = process_batch(vacancies, resumes)

    if results:
        save_results(results)
    else:
        logger.warning("Результаты не были сгенерированы.")


if __name__ == "__main__":
    main()
