# MarketCrawler
Проект для поиска арбитража среди различных маркетплейсов, продающих скины CS:GO.
## Описание
Данный проект развернут на приватном сервере и каждый час собирает информацию по различным торговым площадкам, специализирющимся на торговле товаров из игры CS:GO.
Котировки товаров сохраняются в горячее хранилище (PostgreSQL) после чего запускается алгоритм поиска пар для выгодной купли/продажи (арбитража).
С помощью SparkCluster каждую ночь архивные данные перекладываются в холодное хранилище S3, а также обновляется агрегированная статистика по средним стоимостям товаров и совершенным сделкам.
Мониторинг производится с помощью Grafana и SuperSet.

## Архитектура сервиса
<img width="767" alt="image" src="https://github.com/artem-gorshkov/MarketCrawler/assets/47691597/bf269dc8-9dbc-45c5-a4a3-37005f6d22e2">

## DL model
<img width="331" alt="image" src="https://github.com/artem-gorshkov/MarketCrawler/assets/47691597/a4be57dd-b22e-4721-9e16-c3dafda87a46">
