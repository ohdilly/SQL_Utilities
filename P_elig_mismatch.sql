SELECT cont_num
      ,cpgrp_num
      ,prodset_num
      ,pg_start
      ,pg_end
      ,prod_num
      ,prod_desc
      ,prod_id_pri
      ,prodelig_num
      ,prodelig_num_fix
      ,cppt_num
      ,lastmod
FROM   (SELECT c.cont_num
              ,cp.cpgrp_num
              ,cp.prodset_num
              ,to_char(cp.cpgrp_dt_start, 'mm/dd/yyyy') pg_start
              ,to_char(cp.cpgrp_dt_end, 'mm/dd/yyyy') pg_end
              ,p.prod_num
              ,p.prod_desc
              ,p.prod_id_pri
              ,ct.prodelig_num
              ,ct.lastmod
              ,(SELECT pe.prodelig_num
                FROM   prodelig pe
                WHERE  pe.prodset_num = cp.prodset_num
                AND    pe.prod_num = ct.prod_num
                AND    pe.status_num = 40
                AND    pe.prodelig_dt_end >= ct.cppt_dt_end) prodelig_num_fix
              ,ct.cppt_num
        FROM   cont  c
              ,cpgrp cp
              ,cppt  ct
              ,prod  p
        WHERE  c.cont_num = cp.cont_num
        AND    cp.cpgrp_num = ct.cpgrp_num
        AND    ct.prod_num = p.prod_num
        AND    ct.cppt_dt_start < ct.cppt_dt_end
        AND    (ct.status_num = 60 AND NOT EXISTS
               (SELECT 1
                 FROM   prodelig pe
                 WHERE  pe.prodelig_num = ct.prodelig_num
                 AND    pe.status_num IN (40, 41, 585)))
--                AND    EXISTS (SELECT 1
--                        FROM   prodelig pe
--                        WHERE  pe.prodset_num = cp.prodset_num
--                        AND    pe.prod_num = ct.prod_num
--                        AND    pe.status_num IN (40, 41, 585)
--                        AND    pe.prodelig_dt_end >= ct.cppt_dt_end)
        ORDER  BY c.cont_num
                 ,cp.cpgrp_num
                 ,p.prod_num
                 ,ct.cppt_num DESC)
WHERE  1 = 1
      --AND    prodelig_num_fix IS NOT NULL
AND    cont_num = 79;