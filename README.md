## 📋 Основные возможности

### 🗃️ Базовые операции
| Команда       | Описание                                  | Пример                   |
|---------------|------------------------------------------|--------------------------|
| `SET key val` | Сохраняет значение                       | `SET name Alice`         |
| `GET key`     | Возвращает значение                      | `GET name` → `Alice`     |
| `UNSET key`   | Удаляет ключ                             | `UNSET name`             |
| `COUNTS val`  | Подсчет ключей с указанным значением     | `COUNTS Alice` → `1`     |
| `FIND val`    | Поиск всех ключей по значению            | `FIND Alice` → `name`    |

### 💾 Транзакции
| Команда       | Описание                     |
|---------------|-----------------------------|
| `BEGIN`       | Начало новой транзакции      |
| `ROLLBACK`    | Откат текущей транзакции     |
| `COMMIT`      | Подтверждение изменений      |

### 🏗️ Архитектурные особенности
- **Поддержка разных хранилищ**: In-Memory и Redis
- **Гибкая архитектура**: 
  - Event Bus Реализация паттерна Publisher-Subscriber
  - Транзакции: Поддержка вложенных транзакций с откатом изменений
  - Отдельный обработчик команд
  - Интерфейсы: Четкое разделение через Protocol
  - Хранилища: Две реализации - In-Memory и Redis

## 🚀 Установка и запуск

### Предварительные требования
- Python ≥ 3.11.9
- [UV](https://github.com/astral-sh/uv) (рекомендуется)

```bash
# Установка UV
curl -LsSf https://astral.sh/uv/install.sh | sh

# Клонирование репозитория
git clone https://github.com/DmitriyGrigoriev/console_app.git
cd console_app

# Создание окружения
uv sync

# Запуск приложения
uv run python app.py
```

## Установка pre-commit
```
pip install pre-commit
pre-commit install
```

## 📝 Примеры использования
После запуска вы увидите приглашение ввода >. Вводите команды по одной на строке.
```
> SET name Alice
> GET name
Alice
> BEGIN
> SET name Bob
> GET name
Bob
> ROLLBACK
> GET name
Alice
> END
```
## 🧪 Тестирование
Запуск unit-тестов:
```
uv run python -m unittest test_app.py
```
**Покрытие тестами:**

* Все базовые операции
* Транзакции (включая вложенные)
* Обработку ошибок
* Граничные случаи


**Конфигурация**
Линтинг: Ruff (конфиг в pyproject.toml)
Форматирование: 120 символов на строку

Зависимости:
- Для Redis-режима: redis-py
- Для тестирования: unittest.mock

## ⚙️ Доступные хранилища

1. In-Memory (по умолчанию)
- Быстрая работа
- Данные только в памяти
- Подходит для тестирования

2. Redis
- Персистентное хранение
- Возможность масштабирования
- Требует запущенный сервер Redis

## ⚠️ Ограничения
* Данные хранятся только в оперативной памяти и не сохраняются между запусками
* Нет поддержки пробелов в значениях ключей
* Нет персистентного хранения

## 📜 Лицензия
MIT License © 2024 Dmitriy Grigoriev