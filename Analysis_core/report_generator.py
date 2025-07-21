# -*- coding: utf-8 -*-
import json
import re
import os
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from docx import Document
from docx.shared import Inches, Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH

# JSON data with <b>...</b> tags for bold formatting
report_data = {
  "report_type": "КЗ201",
  "title": {
    "text": "ОТЧЕТ ОБ <b>ЭКСПЛУАТАЦИИ КРИОГЕННОГО ЗАМЕДЛИТЕЛЯ</b>",
    "font_size": 14,
    "alignment": "center"
  },
  "subtitle": {
    "text": "КЗ201 за период <b>14.04.2025 – 25.04.2025</b>",
    "font_size": 12,
    "alignment": "center"
  },
  "period": {
    "start_date": "14.04.2025",
    "end_date": "25.04.2025"
  },
  "sections": [
    {
      "id": 1,
      "title": {"text": "Цикл", "font_size": 10},
      "content": {"text": "<b>14.04.2025 – 25.04.2025</b>", "font_size": 10}
    },
    {
      "id": 2,
      "title": {"text": "Состояние реактора", "font_size": 10},
      "content": {"text": "на мощности <b>1,4 кВт</b>", "font_size": 10}
    },
    {
      "id": 3,
      "title": {"text": "Технологическая схема", "font_size": 10},
      "content": {"text": "В приложении 1", "font_size": 10}
    },
    {
      "id": 4,
      "title": {"text": "Загрузка шариков", "font_size": 10},
      "subsections": [
        {
          "title": {"text": "Фактически загружено", "font_size": 10},
          "table": {
            "headers": ["<b>Партия</b>", "Объем, мл"],
            "rows": [["<b>1</b>", 330], ["<b>2</b>", 330], ["<b>3</b>", 330]],
            "font_size": 10,
            "bold_columns": [],
            "align": "center"
          }
        },
        {
          "title": {"text": "Измерено при помощи УДШ (Узел детектирования шариков)", "font_size": 10},
          "table": {
            "headers": ["<b>Партия</b>", "Зарегистрировано, шт.", "Теоретическое кол-во, шт.", "Погрешность фактическая, %"],
            "rows": [["<b>1</b>", 2628, 9000, 29.2], ["<b>2</b>", 3324, 9000, 36.9], ["<b>3</b>", 3847, 9000, 42.7]],
            "font_size": 10,
            "bold_columns": [],
            "align": "center"
          }
        }
      ]
    },
    {
      "id": 5,
      "title": {"text": "Показания <b>газгольдера</b>", "font_size": 10},
      "table": {
        "headers": ["<b>Объем</b>"],
        "rows": [["2,14"] for _ in range(10)],
        "font_size": 10,
        "bold_columns": [],
        "align": "center"
      }
    },
    {
      "id": 6,
      "title": {"text": "Показания температур", "font_size": 10},
      "content": [
        {
          "text": {"text": "Рисунок 2. Температурные данные с <b>термодиода DT_51</b>", "font_size": 10},
          "image": {"path": "C:\\Users\\Иван Литвак\\Desktop\\photo_2025-03-27_17-04-18.jpg", "width": 400, "height": 200}
        },
        {
          "text": {"text": "Рисунок 4. Температурные данные с <b>термопары Т32</b> в период с 9.00 до 10.00 14.04.2025г.", "font_size": 10},
          "image": {"path": "image4.jpeg", "width": 400, "height": 200}
        }
      ]
    },
    {
      "id": 7,
      "title": {"text": "Показания давления (вакуума)", "font_size": 10},
      "content": [
        {
          "text": {"text": "Рисунок 3. Показания <b>вакуумметрического датчика P22</b> в контуре", "font_size": 10},
          "image": {"path": "image3.png", "width": 400, "height": 200}
        },
        {
          "text": {"text": "Рисунок 5. Показания <b>вакуумметрического датчика ВД21</b> в рубашке", "font_size": 10},
          "image": {"path": "image5.jpeg", "width": 400, "height": 200}
        }
      ]
    },
    {
      "id": 8,
      "title": {"text": "Контроль концентрации радиолитического водорода и кислорода при отогреве", "font_size": 10},
      "subsections": [
        {
          "title": {"text": "Контроль концентрации радиолитического <b>водорода</b>", "font_size": 10},
          "content": {"text": "<b>Не проводилось</b>", "font_size": 10}
        },
        {
          "title": {"text": "Контроль концентрации <b>кислорода</b>", "font_size": 10},
          "content": {"text": "<b>Не проводилось</b>", "font_size": 10}
        }
      ]
    },
    {
      "id": 9,
      "title": {"text": "Мезитилен", "font_size": 10},
      "subsections": [
        {
          "title": {"text": "Объем слитого мезитилена", "font_size": 10},
          "table": {
            "headers": ["<b>Дата</b>", "Объем, мл", "Примечания"],
            "rows": [
              ["<b>05.05.2025</b>", 610, "Из камеры"],
              ["<b>12.05.2025</b>", 10, "Из внутреннего трубопровода"],
              ["<b>15.05.2025</b>", 5, "Из внутреннего трубопровода"],
              ["<b>Всего:</b>", 625, "-"]
            ],
            "font_size": 10,
            "bold_columns": [],
            "align": "center"
          }
        },
        {
          "title": {"text": "Измерение вязкости <b>мезитилена</b>", "font_size": 10},
          "content": {"text": "Вязкость слитого мезитилена <b>КЗ 201</b> - 23 сР", "font_size": 10}
        }
      ]
    },
    {
      "id": 10,
      "title": {"text": "Дополнительные измерения", "font_size": 10},
      "content": [
        {"text": "Излучение по <b>гамма</b>: 19 мкЗв", "font_size": 10},
        {"text": "Излучение по <b>бетта</b>: 2200 частиц", "font_size": 10}
      ],
      "style": "bullet"
    },
    {
      "id": 11,
      "title": {"text": "Результаты", "font_size": 10},
      "content": [
        {"text": "Все работы на <b>КЗ</b> выполнялись в соответствие с утвержденным Планом работ по криогенным замедлителям <b>КЗ201, КЗ202</b> и стенду КЗ201 на период 01.01.2025 – 30.06.2025г. (см. приложение 2).", "font_size": 10},
        {"text": "<b>11.04.2025 г.</b> выполнена подготовка КЗ201 к работе по инструкции, все системы работали штатно.", "font_size": 10},
        {"text": "<b>12.04.2025 г.</b> - охлаждение КЗ прошло штатно.", "font_size": 10},
        {"text": "<b>08:40-12.40 13.04.2025 г.</b> выполнена загрузка трёх партий шариков, загрузка прошла штатно. Общее время загрузки шариков в камеру составило 4 часов.", "font_size": 10},
        {"text": "Около <b>09:10 14.04.2025 г.</b> произошло отключение газодувки в связи с кратковременным обрывом связи <b>modbus</b> с газодувкой, внутри контура с гелием зафиксирован скачок температур до <b>136К</b> (см. рис.4) и падения вакуума в рубашке до <b>0,036 Торр</b> (см. рис.5).", "font_size": 10},
        {"text": "Остановка газодувки не сопровождалась ни выводом ошибки, ни звуковыми, ни световыми сигналами. Около <b>09:37 14.04.2025 г.</b> газодувка была запущена снова и все данные вернулись к штатным значениям.", "font_size": 10},
        {"text": "При отогреве произошел выброс <b>радиоактивности</b>.", "font_size": 10}
      ],
      "style": "bullet"
    },
    {
      "id": 12,
      "title": {"text": "Выводы", "font_size": 10},
      "content": {"text": "[Заполнить]", "font_size": 10}
    },
    {
      "id": 13,
      "title": {"text": "Заключение (рекомендации)", "font_size": 10},
      "content": {"text": "[Заполнить]", "font_size": 10}
    }
  ],
  "developers": [
    {"role": "Начальник группы №2 Сектор НИиКЗ ЛНФ ОИЯИ", "name": "Галушко <b>А.В.</b>", "signature": "____", "font_size": 10},
    {"role": "Ответственный по работам на КЗ201", "name": "Ыскаков <b>А.</b>", "signature": "____", "font_size": 10}
  ],
  "approvers": [
    {"role": "Начальник сектора НИиКЗ ЛНФ ОИЯИ", "name": "Булавин <b>М.В.</b>", "signature": "____", "font_size": 10},
    {"role": "Начальник МТО ЛНФ ОИЯИ", "name": "Слотвицкий <b>Ю.М.</b>", "signature": "____", "font_size": 10},
    {"role": "Начальник группы №2 МТО ЛНФ ОИЯИ", "name": "Скуратов <b>В.А.</b>", "signature": "____", "font_size": 10}
  ],
  "appendices": [
    {
      "title": {"text": "ПРИЛОЖЕНИЕ 1. <b>ТЕХНОЛОГИЧЕСКАЯ СХЕМА</b>", "font_size": 14},
      "content": {"image": {"path": "image6.png", "width": 400, "height": 200}}
    },
    {
      "title": {"text": "ПРИЛОЖЕНИЕ 2. <b>ПРОГРАММА РАБОТ</b>", "font_size": 14},
      "content": {"text": "[Заменитель для программы работ]", "font_size": 10}
    }
  ]
}

# Font registration
font_path = r"C:\Windows\Fonts\times.ttf"
font_bold_path = r"C:\Windows\Fonts\timesbd.ttf"

if not os.path.exists(font_path) or not os.path.exists(font_bold_path):
    raise FileNotFoundError("Шрифты Times New Roman не найдены")

pdfmetrics.registerFont(TTFont("TimesNewRoman", font_path))
pdfmetrics.registerFont(TTFont("TimesNewRoman-Bold", font_bold_path))

def create_pdf_report(data, output_path):
    doc = SimpleDocTemplate(output_path, pagesize=A4)
    styles = getSampleStyleSheet()

    # Dynamic style creation
    def get_paragraph_style(font_size, alignment="left"):
        name = f"CustomStyle_{font_size}_{alignment}"
        if name not in styles:
            align_map = {"left": 0, "center": 1, "right": 2}
            styles.add(ParagraphStyle(
                name=name,
                fontName="TimesNewRoman",
                fontSize=font_size,
                alignment=align_map.get(alignment, 0),
                spaceAfter=12,
                leading=font_size + 2,
                encoding='utf-8'
            ))
        return styles[name]

    def format_table_cell(text, font_size, align="center"):
        text = str(text).replace('<b>', '<b><font name="TimesNewRoman-Bold">').replace('</b>', '</font></b>')
        return Paragraph(text, get_paragraph_style(font_size, align))

    elements = []

    # Title and subtitle
    for item in [data['title'], data['subtitle']]:
        text = item['text'].replace('<b>', '<b><font name="TimesNewRoman-Bold">').replace('</b>', '</font></b>')
        elements.append(Paragraph(text, get_paragraph_style(item['font_size'], item['alignment'])))
    elements.append(Spacer(1, 12))

    # Sections
    for section in data['sections']:
        title_text = section['title']['text'].replace('<b>', '<b><font name="TimesNewRoman-Bold">').replace('</b>', '</font></b>')
        elements.append(Paragraph(
            f"{section['id']}. {title_text}",
            get_paragraph_style(section['title']['font_size'])
        ))

        if 'content' in section:
            if isinstance(section['content'], dict) and 'text' in section['content']:
                text = section['content']['text'].replace('<b>', '<b><font name="TimesNewRoman-Bold">').replace('</b>', '</font></b>')
                elements.append(Paragraph(text, get_paragraph_style(section['content']['font_size'])))
            elif isinstance(section['content'], list):
                for item in section['content']:
                    if isinstance(item, dict) and 'image' in item:
                        text = item['text']['text'].replace('<b>', '<b><font name="TimesNewRoman-Bold">').replace('</b>', '</font></b>')
                        elements.append(Paragraph(text, get_paragraph_style(item['text']['font_size'])))
                        if os.path.exists(item['image']['path']):
                            elements.append(Image(
                                item['image']['path'],
                                width=item['image']['width'],
                                height=item['image']['height']
                            ))
                        else:
                            elements.append(Paragraph(
                                f"[Image not found: {item['image']['path']}]",
                                get_paragraph_style(10)
                            ))
                    else:
                        text = item['text'].replace('<b>', '<b><font name="TimesNewRoman-Bold">').replace('</b>', '</font></b>')
                        prefix = "• " if section.get('style') == 'bullet' else ""
                        elements.append(Paragraph(
                            f"{prefix}{text}",
                            get_paragraph_style(item['font_size'])
                        ))

        if 'table' in section:
            table_data = [[format_table_cell(h, section['table']['font_size'], section['table']['align']) for h in section['table']['headers']]]
            for row in section['table']['rows']:
                table_data.append([format_table_cell(cell, section['table']['font_size'], section['table']['align']) for cell in row])
            table = Table(table_data)
            table.setStyle(TableStyle([
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'TimesNewRoman'),
                ('FONTSIZE', (0, 0), (-1, -1), section['table']['font_size']),
                ('ALIGN', (0, 0), (-1, -1), section['table']['align'].upper()),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke)
            ] + [('FONTNAME', (col, 1), (col, -1), 'TimesNewRoman-Bold') for col in section['table'].get('bold_columns', [])]))
            elements.append(table)

        if 'subsections' in section:
            for subsection in section['subsections']:
                text = subsection['title']['text'].replace('<b>', '<b><font name="TimesNewRoman-Bold">').replace('</b>', '</font></b>')
                elements.append(Paragraph(text, get_paragraph_style(subsection['title']['font_size'])))
                if 'table' in subsection:
                    table_data = [[format_table_cell(h, subsection['table']['font_size'], subsection['table']['align']) for h in subsection['table']['headers']]]
                    for row in subsection['table']['rows']:
                        table_data.append([format_table_cell(cell, subsection['table']['font_size'], subsection['table']['align']) for cell in row])
                    table = Table(table_data)
                    table.setStyle(TableStyle([
                        ('GRID', (0, 0), (-1, -1), 1, colors.black),
                        ('FONTNAME', (0, 0), (-1, -1), 'TimesNewRoman'),
                        ('FONTSIZE', (0, 0), (-1, -1), subsection['table']['font_size']),
                        ('ALIGN', (0, 0), (-1, -1), subsection['table']['align'].upper()),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke)
                    ] + [('FONTNAME', (col, 1), (col, -1), 'TimesNewRoman-Bold') for col in subsection['table'].get('bold_columns', [])]))
                    elements.append(table)
                elif 'content' in subsection:
                    text = subsection['content']['text'].replace('<b>', '<b><font name="TimesNewRoman-Bold">').replace('</b>', '</font></b>')
                    elements.append(Paragraph(text, get_paragraph_style(subsection['content']['font_size'])))
                elements.append(Spacer(1, 6))
        elements.append(Spacer(1, 12))

    # Developers and Approvers
    for title, people in [("РАЗРАБОТАНО:", data['developers']), ("СОГЛАСОВАНО:", data['approvers'])]:
        elements.append(Paragraph(title, get_paragraph_style(10)))
        table_data = [[format_table_cell(h, people[0]['font_size'], 'left') for h in ["Должность", "Подпись", "Ф.И.О."]]]
        for p in people:
            row = [
                format_table_cell(p['role'], people[0]['font_size'], 'left'),
                format_table_cell(p['signature'], people[0]['font_size'], 'left'),
                format_table_cell(p['name'], people[0]['font_size'], 'left')
            ]
            table_data.append(row)
        table = Table(table_data)
        table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 0), (-1, -1), 'TimesNewRoman'),
            ('FONTSIZE', (0, 0), (-1, -1), people[0]['font_size']),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        elements.append(table)
        elements.append(Spacer(1, 12))

    # Appendices
    for appendix in data['appendices']:
        elements.append(PageBreak())
        text = appendix['title']['text'].replace('<b>', '<b><font name="TimesNewRoman-Bold">').replace('</b>', '</font></b>')
        elements.append(Paragraph(text, get_paragraph_style(appendix['title']['font_size'])))
        if isinstance(appendix['content'], dict) and 'image' in appendix['content']:
            if os.path.exists(appendix['content']['image']['path']):
                elements.append(Image(
                    appendix['content']['image']['path'],
                    width=appendix['content']['image']['width'],
                    height=appendix['content']['image']['height']
                ))
            else:
                elements.append(Paragraph(
                    f"[Image not found: {appendix['content']['image']['path']}]",
                    get_paragraph_style(10)
                ))
        else:
            text = appendix['content']['text'].replace('<b>', '<b><font name="TimesNewRoman-Bold">').replace('</b>', '</font></b>')
            elements.append(Paragraph(text, get_paragraph_style(appendix['content']['font_size'])))

    doc.build(elements)
    print(f"Отчет PDF сохранен как {output_path}")

def create_docx_report(data, output_path):
    doc = Document()

    def add_formatted_paragraph(doc, text, font_size, alignment="left", style=None):
        p = doc.add_paragraph(style=style)
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER if alignment == "center" else WD_ALIGN_PARAGRAPH.LEFT
        parts = re.split(r'(<b>.*?</b>)', text)
        for part in parts:
            if part.startswith('<b>') and part.endswith('</b>'):
                run = p.add_run(part[3:-4])
                run.bold = True
            else:
                run = p.add_run(part)
            run.font.size = Pt(font_size)
        return p

    def add_formatted_cell(cell, text, font_size):
        cell.text = ""  # Clear default text
        parts = re.split(r'(<b>.*?</b>)', text)
        for part in parts:
            if part.startswith('<b>') and part.endswith('</b>'):
                run = cell.paragraphs[0].add_run(part[3:-4])
                run.bold = True
            else:
                run = cell.paragraphs[0].add_run(part)
            run.font.size = Pt(font_size)

    # Title and subtitle
    for item in [data['title'], data['subtitle']]:
        add_formatted_paragraph(doc, item['text'], item['font_size'], item['alignment'])
    doc.add_paragraph("")

    # Sections
    for section in data['sections']:
        add_formatted_paragraph(doc, f"{section['id']}. {section['title']['text']}", section['title']['font_size'])
        if 'content' in section:
            if isinstance(section['content'], dict) and 'text' in section['content']:
                add_formatted_paragraph(doc, section['content']['text'], section['content']['font_size'])
            elif isinstance(section['content'], list):
                for item in section['content']:
                    if isinstance(item, dict) and 'image' in item:
                        add_formatted_paragraph(doc, item['text']['text'], item['text']['font_size'])
                        if os.path.exists(item['image']['path']):
                            doc.add_picture(item['image']['path'], width=Inches(item['image']['width'] / 72))
                        else:
                            add_formatted_paragraph(doc, f"[Image not found: {item['image']['path']}]", 10)
                    else:
                        add_formatted_paragraph(doc, item['text'], item['font_size'], style='List Bullet' if section.get('style') == 'bullet' else None)

        if 'table' in section:
            table = doc.add_table(rows=1, cols=len(section['table']['headers']))
            table.style = 'Table Grid'
            for i, header in enumerate(section['table']['headers']):
                add_formatted_cell(table.rows[0].cells[i], header, section['table']['font_size'])
            for row in section['table']['rows']:
                cells = table.add_row().cells
                for i, cell in enumerate(row):
                    add_formatted_cell(cells[i], str(cell), section['table']['font_size'])
                    if i in section['table'].get('bold_columns', []):
                        for run in cells[i].paragraphs[0].runs:
                            run.bold = True

        if 'subsections' in section:
            for subsection in section['subsections']:
                add_formatted_paragraph(doc, subsection['title']['text'], subsection['title']['font_size'])
                if 'table' in subsection:
                    table = doc.add_table(rows=1, cols=len(subsection['table']['headers']))
                    table.style = 'Table Grid'
                    for i, header in enumerate(subsection['table']['headers']):
                        add_formatted_cell(table.rows[0].cells[i], header, subsection['table']['font_size'])
                    for row in subsection['table']['rows']:
                        cells = table.add_row().cells
                        for i, cell in enumerate(row):
                            add_formatted_cell(cells[i], str(cell), subsection['table']['font_size'])
                            if i in subsection['table'].get('bold_columns', []):
                                for run in cells[i].paragraphs[0].runs:
                                    run.bold = True
                elif 'content' in subsection:
                    add_formatted_paragraph(doc, subsection['content']['text'], subsection['content']['font_size'])

    # Developers and Approvers
    for title, people in [("РАЗРАБОТАНО:", data['developers']), ("СОГЛАСОВАНО:", data['approvers'])]:
        add_formatted_paragraph(doc, title, 10)
        table = doc.add_table(rows=1, cols=3)
        table.style = 'Table Grid'
        headers = ["Должность", "Подпись", "Ф.И.О."]
        for i, header in enumerate(headers):
            add_formatted_cell(table.rows[0].cells[i], header, people[0]['font_size'])
        for person in people:
            row = table.add_row().cells
            add_formatted_cell(row[0], person['role'], person['font_size'])
            add_formatted_cell(row[1], person['signature'], person['font_size'])
            add_formatted_cell(row[2], person['name'], person['font_size'])

    # Appendices
    for appendix in data['appendices']:
        doc.add_page_break()
        add_formatted_paragraph(doc, appendix['title']['text'], appendix['title']['font_size'])
        if isinstance(appendix['content'], dict) and 'image' in appendix['content']:
            if os.path.exists(appendix['content']['image']['path']):
                doc.add_picture(appendix['content']['image']['path'], width=Inches(appendix['content']['image']['width'] / 72))
            else:
                add_formatted_paragraph(doc, f"[Image not found: {appendix['content']['image']['path']}]", 10)
        else:
            add_formatted_paragraph(doc, appendix['content']['text'], appendix['content']['font_size'])

    doc.save(output_path)
    print(f"Отчет DOCX сохранен как {output_path}")

def generate_report(data, pdf_output="report.pdf", docx_output="report.docx"):
    create_pdf_report(data, pdf_output)
    create_docx_report(data, docx_output)

if __name__ == "__main__":
    generate_report(report_data, "Отчет_КЗ201_12.04.25-25.04.25.pdf", "Отчет_КЗ201_12.04.25-25.04.25.docx")