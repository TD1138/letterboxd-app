SELECT

	 a.FILM_ID
	 ,COALESCE(julianday('now') - julianday(b.CREATED_AT), 99) AS DAYS_SINCE_LAST_UPDATE

FROM ALL_FILMS a

LEFT JOIN FILM_RELEASE_INFO b
ON a.FILM_ID = b.FILM_ID

LEFT JOIN TMDB_ID c
ON a.FILM_ID = c.FILM_ID

LEFT JOIN FILM_YEAR d
ON a.FILM_ID = d.FILM_ID

WHERE c.VALID = 1
AND DAYS_SINCE_LAST_UPDATE > 7
AND substr(b.FILM_RELEASE_DATE, 0, 5) != d.FILM_YEAR 