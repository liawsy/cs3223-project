# CS3223 Project
## Introduction
This project models a simple SPJ (Select-Project-Join) query engine, simulating query processing in a RDMS environment. The project team comprises:

1. Li Zi Ying
2. Liaw Siew Yee
3. Pang Jia Da

## Implemented Operators
In this project, we have implemented the following specifications:
1. [Block Nested Join](./src/qp/operators/BlockNestedJoin.java)
2. [Sort Merge Join](./src/qp/operators/SortMergeJoin.java) (with [External Sort-Merge algorithm](./src/qp/operators/ExternalSort.java))
3. [Distinct](./src/qp/operators/Distinct.java)
4. [Orderby](./src/qp/operators/Orderby.java) (ASC and DESC)
5. [Groupby](./src/qp/operators/Groupby.java)

For more information about the implementation, please refer to [our report](./Report.pdf).

## Setting up
We will use query2.sql as an example. It is reproduced below:

```sql
SELECT CUSTOMER.cid,CUSTOMER.gender,CUSTOMER.firstname 
FROM CUSTOMER 
WHERE CUSTOMER.gender="1"
```

### Steps:
1. For MAC users, set the classpath in the root folder using: `source queryenv`. For Windows users, set the classpath in the root folder using: `set classpath=%COMPONENT%;%COMPONENT%\classes;%COMPONENT%\lib`.
2. Next, run `build.bat` or `build.sh` in the root folder to compile the java files.
3. For our query, only `CUSTOMER` is used, so we will have to generate the `CUSTOMER` table.
4. Shift the `CUSTOMER.det` file from where ever it is in the project to the `classes\` folder.
5. Enter the `classes\` subfolder. `cd classes`.
6. `java RandomDB CUSTOMER 100` to generate 100 random customer records.
7. `java ConvertTxtToTbl CUSTOMER` to generate some serialized tables.
8. If there are other tables used in the query, repeat steps 4-5 with the other table names of those `.det` files.
9. From the previous 2 commands, there will be `.md`, `.tbl`, `.stat` files created in `classes\`.
10. Inside `classes\`, run `java QueryMain query2.in out.txt 1000 1000` and press Enter.
11. It should successfully run the program and you will see a proposed execution plan. Enter `1` to carry on.
12. The result of the query should be in `out.txt` in the current folder.



