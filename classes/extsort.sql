SELECT DISTINCT *
FROM TTONE
WHERE TTONE.pk>"50"
GROUPBY TTONE.pk, TTONE.c1, TTONE.c2, TTONE.c3
