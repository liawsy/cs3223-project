SELECT *
FROM EMPLOYEES,CERTIFIED,SCHEDULE
WHERE EMPLOYEES.eid=CERTIFIED.eid,CERTIFIED.aid=SCHEDULE.aid