
SELECT
	bunit_num, start_gap, end_gap
FROM
	(
	SELECT
		b.bunit_num,
		max(finish) OVER( PARTITION BY b.bunit_num ORDER BY strt ) + 1 start_gap,
		lead(strt) OVER( PARTITION BY b.bunit_num ORDER BY finish ) - 1 end_gap
	FROM
		(
		SELECT
			bunit_num ,
			count(bunit_num)
		FROM
		phs_test
		GROUP BY
			bunit_num
		HAVING
			count(bunit_num) > 1 ) a,
		(
		SELECT
			BUNIT_num ,
			elig_dt_start strt ,
			elig_dt_end Finish
		FROM
			phs_test ) b
	WHERE
		a.bunit_num = b.bunit_num )
WHERE
	start_gap <= end_gap;
