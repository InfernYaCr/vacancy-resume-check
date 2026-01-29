import json
import os
import glob
import time
from typing import List, Dict, Any, Optional
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_latest_results(reports_dir: str = "reports") -> Optional[List[Dict[str, Any]]]:
    """
    Загружает последний файл результатов analysis_results_*.json из указанной директории.

    Args:
        reports_dir: Директория с файлами отчетов.

    Returns:
        Список словарей с результатами или None, если файлы не найдены.
    """
    try:
        # Шаблоны поиска
        search_pattern = os.path.join(reports_dir, "analysis_results_*.json")
        files = glob.glob(search_pattern)

        # Фолбек на текущую директорию для обратной совместимости
        if not files:
            files = glob.glob("analysis_results_*.json")

        if not files:
            logger.warning("Файлы результатов анализа не найдены.")
            return None

        # Сортировка по времени изменения (по убыванию)
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Загрузка результатов из: {latest_file}")

        with open(latest_file, "r", encoding="utf-8") as f:
            return json.load(f)

    except Exception as e:
        logger.error(f"Ошибка загрузки результатов: {e}", exc_info=True)
        return None


def generate_markdown_report(results: List[Dict[str, Any]]) -> Optional[str]:
    """
    Генерирует структурированный Markdown отчет из результатов анализа.

    Args:
        results: Список словарей с анализом кандидатов.

    Returns:
        Строка с Markdown отчетом или None, если входные данные пусты.
    """
    if not results:
        return None

    timestamp = time.strftime("%Y-%m-%d %H:%M")
    report_lines = [f"# Отчет по кандидатам от {timestamp}", ""]

    # Группировка по вакансиям
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in results:
        vac_file = item.get("vacancy_file", "Неизвестная вакансия")
        if vac_file not in grouped:
            grouped[vac_file] = []
        grouped[vac_file].append(item)

    # Вспомогательная функция для форматирования одного кандидата
    def format_candidate(index: int, cand: Dict[str, Any]) -> List[str]:
        info = cand.get("candidate_info", {})
        scoring = cand.get("scoring", {})
        score = scoring.get("total_score", 0)
        verdict = cand.get("verdict", "N/A")
        name = info.get("name", "Не указано")

        # Эмодзи для оценки
        if score >= 80:
            icon = "🟢"
        elif score >= 50:
            icon = "🟡"
        else:
            icon = "🔴"

        lines = []
        lines.append(f"### {index}. {icon} {name} (Оценка: {score}/100)")
        lines.append(f"**Вердикт:** {verdict}")
        lines.append(f"📄 **Файл:** {cand.get('resume_file', 'N/A')}")
        lines.append("")

        # Таблица баллов
        breakdown = scoring.get("breakdown", {})
        lines.append("| Критерий | Оценка |")
        lines.append("| --- | --- |")
        lines.append(f"| Hard Skills | {breakdown.get('hard_skills', '-')} |")
        lines.append(f"| Опыт | {breakdown.get('experience', '-')} |")
        lines.append(f"| Локация | {breakdown.get('location', '-')} |")
        lines.append(f"| Soft Skills | {breakdown.get('soft_skills_culture', '-')} |")
        lines.append("")

        # Плюсы и минусы
        pros = cand.get("pros", [])
        cons = cand.get("cons", [])

        if pros:
            lines.append("**Плюсы:**")
            for p in pros:
                lines.append(f"- {p}")
            lines.append("")

        if cons:
            lines.append("**Минусы/Риски:**")
            for c in cons:
                lines.append(f"- {c}")
            lines.append("")

        # Обоснование
        reasoning = cand.get("reasoning_chain", "")
        if reasoning:
            lines.append(f"**Обоснование:** {reasoning}")

        lines.append("---")
        return lines

    # Сборка отчета
    for vacancy, candidates in grouped.items():
        report_lines.append(f"## Вакансия: {vacancy}")
        report_lines.append(f"Всего кандидатов: {len(candidates)}")
        report_lines.append("")

        # Сортировка кандидатов по баллу (по убыванию)
        # Безопасное извлечение балла с дефолтом 0
        candidates.sort(
            key=lambda x: (
                x.get("scoring", {}).get("total_score", 0)
                if isinstance(x.get("scoring"), dict)
                else 0
            ),
            reverse=True,
        )

        if not candidates:
            continue

        # ТОП-3
        report_lines.append("### 🏆 ТОП-3 ЛУЧШИХ КАНДИДАТОВ")
        top3 = candidates[:3]
        for i, cand in enumerate(top3, 1):
            report_lines.extend(format_candidate(i, cand))

        report_lines.append("")

        # BOTTOM-3 (Только если > 3 кандидатов)
        if len(candidates) > 3:
            report_lines.append("### 📉 BOTTOM-3 (АУТСАЙДЕРЫ)")
            bottom3 = candidates[-3:]
            # Исключаем дубли, если они уже есть в top3
            bottom3 = [c for c in bottom3 if c not in top3]

            if bottom3:
                for i, cand in enumerate(bottom3, 1):
                    # Вычисляем оригинальный ранг
                    rank = len(candidates) - len(bottom3) + i
                    report_lines.extend(format_candidate(rank, cand))
            else:
                report_lines.append("(Все кандидаты вошли в ТОП-3)")

        report_lines.append("")
        report_lines.append("*" * 50)
        report_lines.append("")

    return "\n".join(report_lines)


def main():
    """Основная функция выполнения."""
    results = load_latest_results()
    if not results:
        logger.warning("Завершение: Нет результатов для обработки.")
        return

    logger.info("Генерация отчета...")
    report_content = generate_markdown_report(results)

    reports_dir = "reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)

    timestamp_filename = time.strftime("%Y%m%d_%H%M%S")
    report_filename = os.path.join(reports_dir, f"report_{timestamp_filename}.md")

    try:
        with open(report_filename, "w", encoding="utf-8") as f:
            f.write(report_content)
        logger.info(f"Отчет успешно создан: {report_filename}")
    except IOError as e:
        logger.error(f"Не удалось записать файл отчета: {e}")


if __name__ == "__main__":
    main()
