SELECT id, order_user_id
FROM users
WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
