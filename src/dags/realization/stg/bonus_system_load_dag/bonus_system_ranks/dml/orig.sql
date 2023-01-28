SELECT id, name, bonus_percent, min_payment_threshold
FROM ranks
WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
