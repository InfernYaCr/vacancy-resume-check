import argparse
import asyncio
import json
import logging
import os
import time
from typing import List, Optional, Tuple, Dict, Any

from openai import AsyncOpenAI, RateLimitError, APIError
from pydantic import ValidationError

from config import config
from extract_mhtml import extract_text_from_mhtml, read_mhtml_file
from hh_parser import HHParser
from generate_report import generate_markdown_report
from models import CandidateAnalysis

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Инициализация асинхронного LLM клиента
client = AsyncOpenAI(api_key=config.LLM_API_KEY, base_url=config.LLM_BASE_URL)

# Семафор для ограничения одновременных запросов (чтобы не превысить Rate Limit)
MAX_CONCURRENT_REQUESTS = 5
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)


async def get_llm_analysis(
    prompt_data: Dict[str, str], prompt_template_name: str = "hr_expert_v2.txt"
) -> Optional[Dict[str, Any]]:
    """
    Асинхронно отправляет запрос в LLM и возвращает анализ кандидата.
    """
    max_retries = 5
    base_delay = 5

    try:
        # Загрузка промпта (синхронная операция, но быстрая)
        prompt_template = config.load_prompt(prompt_template_name)
    except FileNotFoundError:
        logger.error(f"Шаблон промпта {prompt_template_name} не найден.")
        return None

    # Подготовка промпта
    final_prompt = prompt_template
    for key, value in prompt_data.items():
        if value:
            # Ограничение длины только для текстовых (не JSON), или для всех?
            # JSON лучше не резать посередине.
            # Для простоты пока не режем, или режем аккуратно.
            # Если это JSON, то он может быть длинным.
            final_prompt = final_prompt.replace(f"{{{key}}}", str(value))

    async with semaphore:  # Ограничение одновременных вызовов
        for attempt in range(max_retries):
            try:
                # logger.debug(f"Попытка {attempt+1}/{max_retries}...")

                response = await client.chat.completions.create(
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

                # Очистка markdown блоков ```json ... ```
                cleaned_content = _clean_json_content(content)

                try:
                    # Валидация через Pydantic
                    analysis_data = CandidateAnalysis.model_validate_json(
                        cleaned_content
                    )
                    return analysis_data.model_dump()
                except ValidationError as e:
                    logger.error(f"Ошибка валидации Pydantic: {e}")
                    return None
                except json.JSONDecodeError:
                    logger.error(f"Ошибка парсинга JSON: {cleaned_content[:100]}...")
                    return None

            except RateLimitError:
                wait_time = base_delay * (2**attempt)
                logger.warning(f"RateLimit (429). Ждем {wait_time} сек...")
                await asyncio.sleep(wait_time)
            except APIError as e:
                # Обработка 429 от OpenRouter или других провайдеров
                if getattr(e, "code", None) == 429:
                    wait_time = base_delay * (2**attempt)
                    logger.warning(f"API 429. Ждем {wait_time} сек...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"API Error: {e}")
                    raise e
            except Exception as e:
                logger.error(f"Непредвиденная ошибка API: {e}")
                return None

    logger.error("Не удалось получить ответ после всех попыток")
    return None


def _clean_json_content(content: str) -> str:
    """Удаляет markdown обертки из JSON строки."""
    content = content.strip()
    if content.startswith("```"):
        content = content.strip("`")
        if content.startswith("json"):
            content = content[4:]
    return content.strip()


def get_candidate_files(work_dir: str) -> Tuple[List[str], List[str]]:
    """Сканирует директорию и разделяет вакансии и резюме."""
    if not os.path.exists(work_dir):
        logger.error(f"Директория {work_dir} не найдена.")
        return [], []

    files = [f for f in os.listdir(work_dir) if f.lower().endswith(".mhtml")]
    vacancies = []
    resumes = []

    for f in files:
        full_path = os.path.join(work_dir, f)
        if config.VACANCY_KEYWORD.lower() in f.lower():
            vacancies.append(full_path)
        else:
            resumes.append(full_path)

    logger.info(f"Найдено: Вакансий={len(vacancies)}, Резюме={len(resumes)}.")
    return vacancies, resumes


async def process_pair(
    resume_path: str, vacancy_path: str, vacancy_data: Any, parser_type: str, prompt_template: str
) -> Optional[Dict[str, Any]]:
    """Обрабатывает одну пару (Резюме, Вакансия)."""
    resume_filename = os.path.basename(resume_path)
    logger.info(f"Начало анализа: {resume_filename}")

    prompt_data = {}

    if parser_type == "new":
        # Используем HHParser (JSON)
        # vacancy_data уже должен быть dict или json string
        content = read_mhtml_file(resume_path)
        if not content:
            logger.error(f"Не удалось прочитать файл {resume_filename}")
            return None
            
        parser = HHParser()
        resume_json_obj = parser.parse(content)
        
        # Подготовка данных для промпта
        # vacancy_data передается как dict, если parser_type=new
        prompt_data["resume_json"] = json.dumps(resume_json_obj, ensure_ascii=False, indent=2)
        prompt_data["vacancy_json"] = json.dumps(vacancy_data, ensure_ascii=False, indent=2)
        
    else:
        # Используем Old Parser (Markdown)
        resume_text = extract_text_from_mhtml(resume_path)
        if not resume_text:
             logger.error(f"Не удалось извлечь текст из {resume_filename}")
             return None
             
        prompt_data["resume_text"] = resume_text
        prompt_data["vacancy_text"] = vacancy_data # vacancy_data здесь string

    analysis = await get_llm_analysis(prompt_data, prompt_template_name=prompt_template)

    if analysis:
        # Обогащение метаданными
        analysis["vacancy_file"] = os.path.basename(vacancy_path)
        analysis["resume_file"] = resume_filename

        score = analysis.get("scoring", {}).get("total_score", "N/A")
        logger.info(f"✅ Готово: {resume_filename} (Score: {score})")
        return analysis
    else:
        logger.warning(f"❌ Провал: {resume_filename}")
        return None


async def process_batch_async(
    vacancies: List[str], resumes: List[str], parser_type: str, prompt_template: str
) -> List[Dict[str, Any]]:
    """Параллельный запуск анализа для всех комбинаций."""
    tasks = []

    for vacancy_path in vacancies:
        logger.info(f"--- Подготовка вакансии: {os.path.basename(vacancy_path)} ---")
        
        vacancy_data = None
        if parser_type == "new":
            content = read_mhtml_file(vacancy_path)
            if content:
                parser = HHParser()
                vacancy_data = parser.parse(content)
        else:
            vacancy_data = extract_text_from_mhtml(vacancy_path)

        if not vacancy_data:
            logger.error(f"Пропуск вакансии {vacancy_path} (нет данных)")
            continue

        for resume_path in resumes:
            task = asyncio.create_task(
                process_pair(resume_path, vacancy_path, vacancy_data, parser_type, prompt_template)
            )
            tasks.append(task)

    logger.info(f"Запуск {len(tasks)} задач анализа параллельно...")
    results = await asyncio.gather(*tasks)

    # Фильтрация успешных результатов (удаляем None)
    valid_results = [r for r in results if r is not None]
    return valid_results


def save_results(results: List[Dict[str, Any]], reports_dir: str = "reports") -> None:
    """Сохраняет результаты и генерирует отчет."""
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(reports_dir, f"analysis_results_{timestamp}.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    logger.info(f"\nРезультаты сохранены: {output_file}")

    logger.info("Генерация отчета...")
    report_content = generate_markdown_report(results)
    if report_content:
        report_filename = os.path.join(reports_dir, f"report_{timestamp}.md")
        with open(report_filename, "w", encoding="utf-8") as f:
            f.write(report_content)
        logger.info(f"📄 Отчет создан: {report_filename}")


async def async_main():
    parser = argparse.ArgumentParser(description="AI Recruitment Assistant")
    parser.add_argument("--parser", choices=["old", "new"], default="new", help="Type of parser to use (old=text, new=json)")
    parser.add_argument("--prompt", default=None, help="Prompt template filename (defaults to hr_expert_json.txt for new, hr_expert_legacy_markdown.txt for old)")
    
    args = parser.parse_args()
    
    # Auto-select prompt if not provided
    if args.prompt is None:
        if args.parser == "new":
            args.prompt = "hr_expert_json.txt"
        else:
            args.prompt = "hr_expert_legacy_markdown.txt"
    
    work_dir = "resume vs vacancy"
    vacancies, resumes = get_candidate_files(work_dir)

    if not vacancies or not resumes:
        logger.warning("Нет файлов для обработки.")
        return

    logger.info(f"Запуск анализа. Парсер: {args.parser}, Промпт: {args.prompt}")
    start_time = time.time()

    results = await process_batch_async(vacancies, resumes, args.parser, args.prompt)

    duration = time.time() - start_time
    logger.info(f"\n=== Обработка завершена за {duration:.2f} сек. ===")
    logger.info(f"Успешно обработано: {len(results)}")

    if results:
        save_results(results)


def main():
    """Точка входа для запуска через 'python analyze_candidates.py'"""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
