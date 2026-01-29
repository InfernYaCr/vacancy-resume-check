import email
from bs4 import BeautifulSoup
import chardet
import os
import re
from markdownify import markdownify as md
from typing import Optional, List, Union
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MHTMLParser:
    """
    Парсит MHTML файлы для извлечения структурированного текста (Markdown) из резюме и вакансий.
    """

    def __init__(self):
        self.stop_phrases_exact = [
            "Откликнуться",
            "Показать контакты",
            "Показать на большой карте",
            "© Яндекс Условия использования",
            "Оценка Dream Job",
            "Рекомендуют работодателя",
            "Все отзывы на Dream Job",
            "Другие вакансии",
            "Похожие вакансии",
        ]
        self.skip_sections = [
            "Задайте вопрос работодателю",
            "Чему можно научиться, пока вы в поиске",
        ]
        self.garbage_selectors = [
            ".bloko-button",
            ".resume-sidebar",
            ".vacancy-sidebar",
            ".supernova-overlay",
            ".header",
            ".footer",
            ".bloko-modal",
            ".cookie-warning",
            ".navi",
            ".top-menu",
            "[data-qa='vacancy-response-section']",
            ".vacancy-address-map",
            ".vacancy-contacts__map",
            ".recommended-vacancies",
            ".similar-vacancies",
        ]

    def parse(self, file_path: str) -> Optional[str]:
        """
        Основная точка входа для парсинга MHTML файла.
        """
        try:
            html_content = self._read_and_decode(file_path)
            if not html_content:
                logger.warning(f"Не удалось прочитать контент из {file_path}")
                return None

            soup = BeautifulSoup(html_content, "html.parser")
            self._clean_soup(soup)

            # Определение типа: вакансия или резюме
            if soup.find(attrs={"data-qa": "vacancy-description"}):
                markdown_text = self._parse_vacancy(soup)
            else:
                markdown_text = self._parse_resume(soup)

            return self._finalize_text(markdown_text)

        except Exception as e:
            logger.error(f"Ошибка при парсинге файла {file_path}: {e}", exc_info=True)
            return None

    def _read_and_decode(self, file_path: str) -> Optional[str]:
        """Читает MHTML файл и возвращает декодированный HTML."""
        try:
            with open(file_path, "rb") as f:
                msg = email.message_from_binary_file(f)

            html_content = None
            charset = None

            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/html":
                        payload = part.get_payload(decode=True)
                        if payload:
                            html_content = payload
                            charset = part.get_content_charset()
                            break
            else:
                if msg.get_content_type() == "text/html":
                    html_content = msg.get_payload(decode=True)
                    charset = msg.get_content_charset()

            if not html_content:
                return None

            # Попытка декодирования
            candidate_encodings = [charset] if charset else []
            candidate_encodings.extend(["utf-8", "windows-1251"])

            for enc in candidate_encodings:
                try:
                    return html_content.decode(enc)
                except (UnicodeDecodeError, TypeError, LookupError):
                    continue

            # Fallback определение кодировки
            detected = chardet.detect(html_content)
            encoding = detected["encoding"]
            if encoding:
                try:
                    return html_content.decode(encoding)
                except Exception:
                    pass

            # Последняя попытка
            try:
                return html_content.decode("utf-8", errors="ignore")
            except Exception:
                return None

        except Exception as e:
            logger.error(f"Ошибка ввода-вывода при чтении {file_path}: {e}")
            return None

    def _clean_soup(self, soup: BeautifulSoup) -> None:
        """Удаляет скрипты, стили и мусорные элементы из Soup (in-place)."""
        # Стандартная очистка
        for tag in soup(
            [
                "script",
                "style",
                "meta",
                "noscript",
                "iframe",
                "svg",
                "path",
                "defs",
                "symbol",
                "link",
                "object",
                "embed",
            ]
        ):
            tag.decompose()

        # Удаление навигации
        for tag in soup(["nav", "footer", "aside"]):
            if tag.parent:
                tag.decompose()

        # Удаление мусора по CSS селекторам
        full_selector = ", ".join(self.garbage_selectors)
        for tag in soup.select(full_selector):
            if tag.parent:
                tag.decompose()

    def _parse_vacancy(self, soup: BeautifulSoup) -> str:
        """Парсинг структуры вакансии."""
        parts = []

        # 1. Заголовок
        title_el = soup.find(attrs={"data-qa": "vacancy-title"}) or soup.find(
            "h1", attrs={"data-qa": "title"}
        )
        if title_el:
            parts.append(f"# {title_el.get_text(strip=True)}")

        # 2. Зарплата
        salary_el = soup.find(attrs={"data-qa": "vacancy-salary"})
        if salary_el:
            salary_text = salary_el.get_text(separator=" ", strip=True)
            salary_text = re.sub(r"\s+", " ", salary_text)
            parts.append(f"**Зарплата:** {salary_text}")

        # 3. Краткая информация
        exp_el = soup.find(attrs={"data-qa": "vacancy-experience"})
        if exp_el:
            info_container = exp_el.parent
            # Ищем контейнер выше (эвристика)
            if info_container and info_container.name != "div":
                info_container = info_container.parent

            if info_container:
                info_md = md(str(info_container), strip=["a", "img"]).strip()
                clean_info_lines = [
                    line
                    for line in info_md.splitlines()
                    if "вакансию смотрят" not in line and "человек" not in line
                ]
                info_md_clean = "\n".join(clean_info_lines).strip()
                if info_md_clean:
                    parts.append("### Краткие условия")
                    parts.append(info_md_clean)

        # 4. Описание
        vacancy_description = soup.find(attrs={"data-qa": "vacancy-description"})
        if vacancy_description:
            parts.append("### Описание вакансии")
            desc_md = md(
                str(vacancy_description), heading_style="ATX", strip=["img", "a"]
            )
            parts.append(desc_md)

        # 5. Ключевые навыки
        skills_els = soup.find_all(attrs={"data-qa": "skills-element"})
        if skills_els:
            parts.append("### Ключевые навыки")
            for skill in skills_els:
                parts.append(f"* {skill.get_text(strip=True)}")

        return "\n\n".join(parts)

    def _parse_resume(self, soup: BeautifulSoup) -> str:
        """Парсинг структуры резюме."""
        # Извлекаем хидер (Фото, Имя, Возраст) отдельно
        resume_header_md = ""
        resume_header = soup.find(class_="resume-header-title")
        if resume_header:
            resume_header_md = md(
                str(resume_header), heading_style="ATX", strip=["img", "a"]
            )
            resume_header.decompose()  # Чтобы не дублировать

        # Изолируем основной контент
        target = soup
        main_zones = [
            soup.find("div", class_="resume-wrapper"),
            soup.find("div", class_="main-content"),
            soup.find(id="app"),
        ]

        for zone in main_zones:
            if zone:
                target = zone
                break

        body_md = md(str(target), heading_style="ATX", strip=["img", "a"])
        return (resume_header_md + "\n\n" + body_md).strip()

    def _finalize_text(self, text: str) -> str:
        """Пост-обработка: очистка текста, удаление стоп-фраз и расстановка отступов."""
        lines = text.splitlines()
        clean_lines = []
        in_skip_section = False

        for line in lines:
            line_str = line.strip()

            # 1. Пропуск пустых строк
            if not line_str:
                continue

            # 2. Фильтрация технического мусора
            if line_str.startswith("{") and "trl" in line_str and len(line_str) > 100:
                continue
            if len(line_str) > 5000:
                continue

            # 3. Стоп-фразы
            if any(
                phrase.lower() in line_str.lower() for phrase in self.stop_phrases_exact
            ):
                continue

            if "Вакансия опубликована" in line_str:
                continue

            # 4. Пропуск секций
            if line_str.startswith("#"):
                header_text = line_str.lstrip("#").strip()
                header_text_norm = re.sub(r"\s+", " ", header_text)

                if any(
                    s.lower() in header_text_norm.lower() for s in self.skip_sections
                ):
                    in_skip_section = True
                else:
                    in_skip_section = False

            if in_skip_section:
                continue

            # 5. Умные отступы для заголовков
            if line_str.startswith("#") and clean_lines:
                clean_lines.append("")

            clean_lines.append(line_str)

        return "\n".join(clean_lines).strip()


# Вспомогательная функция для обратной совместимости
def extract_text_from_mhtml(file_path: str) -> Optional[str]:
    parser = MHTMLParser()
    return parser.parse(file_path)


if __name__ == "__main__":
    # Тестовый запуск
    test_dir = "resume vs vacancy"
    if os.path.exists(test_dir):
        print(f"Тестирование в директории: {test_dir}")
        for filename in os.listdir(test_dir):
            if filename.lower().endswith(".mhtml"):
                path = os.path.join(test_dir, filename)
                print(f"--- Обработка {filename} ---")
                text = extract_text_from_mhtml(path)
                if text:
                    print(f"Успех! Длина: {len(text)} символов")
                    print(text[:200] + "...")
                else:
                    print("Не удалось извлечь текст.")
    else:
        print(
            f"Тестовая директория '{test_dir}' не найдена. Пожалуйста, создайте её и добавьте .mhtml файлы."
        )
