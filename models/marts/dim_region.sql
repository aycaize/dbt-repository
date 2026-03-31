with regions as (
    select * from (values
        (1,  'Akdeniz',          'Güney'),
        (2,  'Ege',              'Batı'),
        (3,  'İç Anadolu',       'İç'),
        (4,  'Marmara',          'Batı'),
        (5,  'Karadeniz',        'Kuzey'),
        (6,  'Doğu Anadolu',     'Doğu'),
        (7,  'Güneydoğu Anadolu','Doğu')
    ) as t(region_id, region_name, geographic_group)
)

select * from regions