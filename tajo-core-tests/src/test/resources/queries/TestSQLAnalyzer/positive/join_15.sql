select  * from x join y on x.id = y.id join (select * from a join b  on a.age = b.age join c on a.sex = c.sex) as ss on x.id = ss.id