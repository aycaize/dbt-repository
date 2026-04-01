with generation_sources as (
    select * from (values
        (1, 'Doğalgaz',          'Fosil',        false),
        (2, 'Rüzgar',            'Yenilenebilir', true),
        (3, 'Güneş',             'Yenilenebilir', true),
        (4, 'Hidrolik',          'Yenilenebilir', true),
        (5, 'Linyit',            'Fosil',        false),
        (6, 'Taşkömürü',         'Fosil',        false),
        (7, 'Nükleer',           'Nükleer',      false),
        (8, 'Jeotermal',         'Yenilenebilir', true),
        (9, 'Biyokütle',         'Yenilenebilir', true),
        (10, 'Fuel Oil',         'Fosil',        false),
        (11, 'Motorin',          'Fosil',        false),
        (12, 'İthalat',          'İthalat',      false),
        (13, 'Diğer',            'Diğer',        false)
    ) as t(source_id, source_name, source_category, is_renewable)
)

select * from generation_sources