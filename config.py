import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()


class Config:
    LLM_API_KEY = os.getenv("LLM_API_KEY") or os.getenv(
        "OPENAI_API_KEY"
    )  # Поддержка обоих вариантов
    LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
    LLM_MODEL = os.getenv("LLM_MODEL", "openai/gpt-4o")
    LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0"))
    # Константа для определения файлов вакансий (имя файла должно содержать это слово)
    VACANCY_KEYWORD = "Вакансия"

    @staticmethod
    def load_prompt(prompt_name="hr_expert_v1.txt"):
        """Загружает промпт из директории prompts/."""
        base_dir = os.path.dirname(os.path.abspath(__file__))
        prompt_path = os.path.join(base_dir, "prompts", prompt_name)

        try:
            with open(prompt_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Файл промпта не найден: {prompt_path}")


config = Config()
