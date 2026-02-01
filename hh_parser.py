import re
import email
from email.policy import default
from bs4 import BeautifulSoup

class HHParser:
    """
    Парсер резюме и вакансий HeadHunter из MHTML/HTML контента.
    """

    def parse(self, content: str) -> dict:
        """
        Основная точка входа. Определяет, является ли контент MHTML (MIME) или "сырым" HTML,
        извлекает HTML-часть и затем парсит её.
        """
        html_content = self._extract_html_from_mhtml(content)
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Простая эвристика для определения типа документа
        title_tag = soup.find('title')
        title_text = title_tag.get_text().lower() if title_tag else ""
        
        # Вакансии обычно имеют слово "Вакансия" в заголовке или специфические блоки
        if "вакансия" in title_text or soup.find(attrs={"data-qa": "vacancy-title"}) or soup.find(attrs={"data-qa": "title"}):
            return self._parse_vacancy(soup)
        else:
            return self._parse_resume(soup)

    def _extract_html_from_mhtml(self, content: str) -> str:
        """
        Парсит MHTML строку с помощью библиотеки email для получения декодированного HTML.
        Мы принудительно используем UTF-8, так как HH.ru всегда в UTF-8, а MHTML заголовки могут врать или отсутствовать.
        """
        if "MIME-Version:" not in content and "Content-Type:" not in content:
             return content # Считаем, что это "сырой" HTML

        msg = email.message_from_string(content, policy=default)
        
        # Проходим по частям сообщения, чтобы найти text/html
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    try:
                        return payload.decode('utf-8')
                    except UnicodeDecodeError:
                        return payload.decode('utf-8', errors='replace')
        
        # Если часть html не найдена, возможно, все сообщение является html?
        if msg.get_content_type() == "text/html":
             payload = msg.get_payload(decode=True)
             if payload:
                 try:
                     return payload.decode('utf-8')
                 except UnicodeDecodeError:
                     return payload.decode('utf-8', errors='replace')

        return content # Возвращаем оригинал, если логика извлечения не сработала

    def _extract_text(self, element) -> str:
        """Вспомогательный метод для извлечения и очистки текста из элемента."""
        if not element:
            return ""
        return element.get_text(separator=" ", strip=True)

    def _parse_resume(self, soup) -> dict:
        data = {
            "type": "resume",
            "name": "", 
            "title": "",
            "salary": "",
            "gender": "",
            "age": "",
            "birth_date": "", # Added
            "area": "",
            "relocation": "", # Added to capture relocation/business trip status
            "metro": "", # Added
            "specializations": [], # Added
            "employment_modes": [],
            "experience_total": "",
            "experience_items": [],
            "education_items": [],
            "language_items": [],
            "skills": [],
            "driver_experience": "",
            "about": "",
            "citizenship": []
        }

        # 1. Шапка (Заголовок, Должность и т.д.)
        data['name'] = self._extract_text(soup.find(attrs={"data-qa": "bloko-header-1"}))
        data['title'] = self._extract_text(soup.find(attrs={"data-qa": "resume-block-title-position"}))
        data['salary'] = self._extract_text(soup.find(attrs={"data-qa": "resume-block-salary"}))
        
        # Личные данные
        data['gender'] = self._extract_text(soup.find(attrs={"data-qa": "resume-personal-gender"}))
        data['age'] = self._extract_text(soup.find(attrs={"data-qa": "resume-personal-age"}))
        
        # Дата рождения
        birthday_el = soup.find(attrs={"data-qa": "resume-personal-birthday"})
        if birthday_el:
             data['birth_date'] = self._extract_text(birthday_el)

        # Адрес, Метро, Переезд
        data['area'] = self._extract_text(soup.find(attrs={"data-qa": "resume-personal-address"}))
        
        # Метро
        # 1. Попытка найти по data-qa
        metro_el = soup.find(attrs={"data-qa": "resume-personal-metro"})
        # 2. Попытка найти по классу
        if not metro_el:
            metro_el = soup.find(class_="metro-station")
        
        if metro_el:
            data['metro'] = self._extract_text(metro_el)

        # Переезд и командировки. Они часто лежат в параграфе рядом с адресом.
        # Ищем родительский контейнер адреса и смотрим текст
        addr_el = soup.find(attrs={"data-qa": "resume-personal-address"})
        if addr_el and addr_el.parent:
            full_personal_text = self._extract_text(addr_el.parent)
            # Эвристика: разбиваем по запятым и ищем ключи
            parts = [p.strip() for p in full_personal_text.split(',')]
            relocation_parts = [p for p in parts if "переезду" in p.lower() or "командировкам" in p.lower()]
            if relocation_parts:
                data['relocation'] = ", ".join(relocation_parts)

        # 2. Занятость / График / Специализации
        spec_cat = soup.find(attrs={"data-qa": "resume-block-specialization-category"})
        if spec_cat:
            container = spec_cat.find_parent(class_="resume-block-container")
            if container:
                data['employment_modes'] = [self._extract_text(p) for p in container.find_all('p')]
                # Специализации
                specs = container.find_all(attrs={"data-qa": "resume-block-position-specialization"})
                data['specializations'] = [self._extract_text(s) for s in specs]

        # 3. Опыт работы
        data['experience_total'] = self._extract_text(soup.find(class_="resume-block__title-text_sub"))
        
        # Элементы опыта
        exp_container = soup.find(attrs={"data-qa": "resume-block-experience"})
        if exp_container:
            items = exp_container.find_all(class_="resume-block-item-gap")
            for item in items:
                period = self._extract_text(item.find(class_="bloko-column_l-2")) 
                
                # Детали в правой колонке
                right_col = item.find(class_="bloko-column_l-10")
                if not right_col: 
                    continue
                    
                company = self._extract_text(right_col.find(attrs={"data-qa": "resume-block-experience-employer"}))
                if not company:
                    company_el = right_col.find(class_="bloko-text_strong")
                    if company_el:
                        company = self._extract_text(company_el)

                # Дополнительная инфо: Сайт и Индустрия
                website = ""
                industry = ""
                
                # Сайт обычно ссылка внутри блока
                link = right_col.find('a', href=True)
                if link:
                    href = link['href']
                    # Фильтруем внутренние ссылки HH (обычно javascript:void(0) или /employer/...)
                    # Но внешние ссылки обычно прямые. Проверяем, что это не пустая заглушка.
                    if "hh.ru" not in href and "javascript" not in href:
                         website = href
                    # Или текст ссылки похож на домен
                    if "." in self._extract_text(link) and not website:
                        website = self._extract_text(link)

                # Индустрия
                ind_el = right_col.find(class_="resume-block__experience-industries")
                if ind_el:
                    industry = self._extract_text(ind_el)

                position = self._extract_text(right_col.find(attrs={"data-qa": "resume-block-experience-position"}))
                description = self._extract_text(right_col.find(attrs={"data-qa": "resume-block-experience-description"}))
                
                if not company and not position and not description:
                     description = self._extract_text(right_col)

                data['experience_items'].append({
                    "period": period,
                    "company": company,
                    "website": website, 
                    "industry": industry, 
                    "position": position,
                    "description": description
                })

        # 4. Образование
        edu_container = soup.find(attrs={"data-qa": "resume-block-education"})
        if edu_container:
             items = edu_container.find_all(class_="resume-block-item-gap")
             seen_education = set()
             for item in items:
                 year = self._extract_text(item.find(class_="bloko-column_l-2"))
                 right_col = item.find(class_="bloko-column_l-10")
                 if right_col:
                     name = self._extract_text(right_col.find(attrs={"data-qa": "resume-block-education-name"}))
                     details = self._extract_text(right_col.find(attrs={"data-qa": "resume-block-education-organization"}))
                     
                     # Simple deduplication key
                     key = (year, name, details)
                     if key not in seen_education:
                        seen_education.add(key)
                        data['education_items'].append({
                            "year": year,
                            "name": name,
                            "details": details
                        })

        # 5. Навыки
        skills_container = soup.find(attrs={"data-qa": "skills-table"})
        if not skills_container:
            header = soup.find(lambda tag: tag.name == "h2" and "навыки" in tag.get_text().lower())
            if header:
                skills_container = header.find_parent(class_="resume-block")

        if skills_container:
            tags = skills_container.find_all(attrs={"data-qa": "bloko-tag__text"})
            if not tags:
                tags = skills_container.find_all(class_="bloko-tag__section_text")
            
            data['skills'] = [self._extract_text(tag) for tag in tags if "научиться" not in self._extract_text(tag).lower()]

        # 6. Языки
        lang_block = soup.find(attrs={"data-qa": "resume-block-languages"})
        if lang_block:
            items = lang_block.find_all(attrs={"data-qa": "resume-block-language-item"})
            data['language_items'] = [self._extract_text(i) for i in items]

        # 7. Опыт вождения
        driver = soup.find(attrs={"data-qa": "resume-block-driver-experience"})
        if driver:
            data['driver_experience'] = self._extract_text(driver)

        # 8. Обо мне
        about = soup.find(attrs={"data-qa": "resume-block-skills-content"})
        if about:
             data['about'] = self._extract_text(about)

        # 9. Гражданство и прочее
        cit_block = soup.find(attrs={"data-qa": "resume-block-additional"})
        if cit_block:
            # Берем текст из <p>, так как там "Гражданство: Россия" и т.д.
            data['citizenship'] = [self._extract_text(p) for p in cit_block.find_all('p')]

        return data

    def _parse_vacancy(self, soup) -> dict:
        data = {
            "type": "vacancy",
            "title": "",
            "salary": "",
            "experience": "",
            "schedule": [],
            "description": "",
            "skills": [],
            "company": "",
            "address": ""
        }

        # 1. Шапка (Заголовок, Зарплата)
        data['title'] = self._extract_text(soup.find(attrs={"data-qa": "vacancy-title"}))
        if not data['title']:
             data['title'] = self._extract_text(soup.find(attrs={"data-qa": "title"})) # Брендированный фоллбэк

        data['salary'] = self._extract_text(soup.find(attrs={"data-qa": "vacancy-salary"}))

        # 2. Условия (Опыт, График, Формат)
        # Стратегия: Находим 'vacancy-experience', затем смотрим соседние элементы в родителе
        exp_node = soup.find(attrs={"data-qa": "vacancy-experience"})
        if exp_node:
            data['experience'] = self._extract_text(exp_node)
            
            # Навигация вверх к контейнеру с условиями. 
            # Обычно структура: Container -> P -> Span(data-qa=vacancy-experience)
            # Иногда Container -> Div -> Span
            # Мы ищем контейнер, который содержит соседей с информацией о графике.
            
            current = exp_node
            container = None
            
            # Поднимаемся вверх на 3 уровня, проверяя, есть ли там нужные keywords в других детях
            for _ in range(3):
                parent = current.find_parent()
                if not parent:
                    break
                
                # Проверяем текст родителя на наличие ключевых слов
                full_text = self._extract_text(parent)
                if any(k in full_text.lower() for k in ['занятость', 'график', 'формат', 'часы']):
                     container = parent
                     # Если нашли родителя, который охватывает всё, можно остановиться, 
                     # но лучше подняться повыше, если это только одна строка.
                     # Эвристика: если родитель содержит много переносов строк или тегов P/Div
                     if len(parent.find_all(['p', 'div'])) > 1:
                         break
                current = parent

            if container:
                # Извлекаем уникальные текстовые фрагменты из контейнера
                seen_texts = set()
                # Сначала добавляем сам опыт, чтобы не дублировать
                seen_texts.add(data['experience'])
                if "опыт работы" in data['experience'].lower():
                     seen_texts.add(data['experience'])
                
                # Итерируемся по всем текстовым узлам или мелким блокам
                # Ищем span/p/div, но исключаем слишком большие блоки (на всякий случай)
                for tag in container.descendants:
                    if tag.name in ['span', 'p', 'div'] and not tag.find_all(['p', 'div']): # Листовые или почти листовые
                        text = self._extract_text(tag)
                        if not text: continue
                        if len(text) > 100: continue # Слишком длинный текст вряд ли график
                        
                        # Очистка от "Опыт работы:" префиксов если они размазаны
                        clean_text = text.replace("Опыт работы:", "").strip()
                        
                        if clean_text and clean_text not in seen_texts:
                            # Проверяем на ключевые слова
                            if any(k in text.lower() for k in ['занятость', 'график', 'формат', 'часы', 'рабочие']):
                                 data['schedule'].append(clean_text)
                                 seen_texts.add(clean_text)
                                 seen_texts.add(text)
        
        # 3. Описание
        desc_node = soup.find(attrs={"data-qa": "vacancy-description"})
        if desc_node:
            data['description'] = self._extract_text(desc_node)

        # 4. Навыки
        # Ищем h2 "Ключевые навыки"
        skills_header = soup.find(lambda tag: tag.name == 'h2' and 'ключевые навыки' in tag.get_text().lower())
        if skills_header:
            # В брендированных шаблонах навыки могут быть в списке ul -> li[data-qa="skills-element"]
            skills_elements = soup.find_all(attrs={"data-qa": "skills-element"})
            if skills_elements:
                data['skills'] = [self._extract_text(el) for el in skills_elements]
            else:
                # Фоллбэк для стандартного шаблона
                tags = soup.find_all(attrs={"data-qa": "bloko-tag__text"})
                data['skills'] = [self._extract_text(t) for t in tags]

        # 5. Компания
        # Первичный поиск
        comp = self._extract_text(soup.find(attrs={"data-qa": "vacancy-company-name"}))
        if comp:
            data['company'] = comp
        else:
            # Фоллбэк: парсинг тега title
            title_tag = soup.find('title')
            if title_tag:
                # Формат обычно: "Вакансия Название - Компания - ..."
                t_text = title_tag.get_text()
                # RegEx для "работа в компании X"
                match = re.search(r"работа в компании\s+(.+?),", t_text, re.IGNORECASE)
                if match:
                    data['company'] = match.group(1).strip('_').strip()
                else:
                    # Грубое разбиение
                    parts = t_text.split(',')
                    if len(parts) > 1:
                         data['company'] = parts[1].strip()

        # 6. Адрес
        data['address'] = self._extract_text(soup.find(attrs={"data-qa": "vacancy-view-location"}))
        if not data['address']:
             data['address'] = self._extract_text(soup.find(class_="vacancy-creation-time-redesigned"))

        return data
