with price_types as (
    select * from (values
        (1, 'PTF', 'Piyasa Takas Fiyatı', 'Arz ve talebin kesiştiği saatlik denge fiyatı'),
        (2, 'SMF', 'Sistem Marginal Fiyatı', 'Dengesizlik durumunda oluşan fiyat'),
        (3, 'PTF-SMF', 'Fiyat Farkı', 'PTF ile SMF arasındaki fark')
    ) as t(price_type_id, price_type_code, price_type_name, description)
)

select * from price_types