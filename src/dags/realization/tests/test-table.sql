create table public_test.testing_result
(
    id  integer generated always as identity primary key,
    test_date_time timestamp not null,
    test_name      text      not null,
    test_result    boolean   not null
);
