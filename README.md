## How to run the sample queries

We will use query2.sql as an example. It is reproduced below:

```sql
SELECT CUSTOMER.cid,CUSTOMER.gender,CUSTOMER.firstname 
FROM CUSTOMER 
WHERE CUSTOMER.gender="1"
```

### Steps:
1. The tables queried is only `CUSTOMER`, so we will have to generate the `CUSTOMER` table.
2. Shift the `CUSTOMER.det` file from whever it is in the project to the `classes\` folder
3. Enter the `classes\` subfolder. `cd classes`
4. `java RandomDB CUSTOMER 100` to generate 100 random customer records
5. `java ConvertTxtToTbl CUSTOMER` to generate some serialized tables
6. If there are other tables used in the query, repeat steps 4-5 with the other table names of those `.det` files
7. From the previous 2 commands, there will be `.md`, `.tbl`, `.stat` files created in `classes\`
8. Copy all those files and the `.md` files into `testcases\`. There should be 2 copies of all those files, 1 in `testcases\` and 1 in `classes\`
9. inside `classes\`, run `java QueryMain query2.in out.txt 1000 1000` and press Enter
10. It should successfully run the program and you will see a proposed execution plan. Enter `1` to carry on.
11. The result of the query should be in `out.txt` in the current folder.