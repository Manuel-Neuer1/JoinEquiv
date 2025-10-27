package sqlancer.mariadb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBBugs;
import sqlancer.mariadb.MariaDBSchema;
import sqlancer.mariadb.MariaDBSchema.MariaDBDataType;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable.MariaDBEngine;
import sqlancer.mariadb.ast.MariaDBVisitor;

public class MariaDBTableGenerator {
    /* 这个类的唯一目的就是以程序化和随机化的方式，生成一个用于创建 MariaDB 表的 SQL CREATE TABLE 语句 */


    private final StringBuilder sb = new StringBuilder(); // 最终拼凑出完整的 SQL 查询
    private final String tableName; // 要创建的表的名字，由外部传入
    private final MariaDBSchema s; // 一个代表当前数据库“状态”的对象。这个生成器需要通过它来了解数据库中已经存在哪些表
    private PrimaryKeyState primaryKeyState = Randomly.fromOptions(PrimaryKeyState.values()); // 一个内部枚举，用来随机决定主键的创建方式。有三种可能
    private final List<String> columnNames = new ArrayList<>(); // 存储在此次生成过程中所有被创建的列名。这对于创建表级主键（需要从中挑选列名）非常重要
    private final Randomly r;
    private final ExpectedErrors errors = new ExpectedErrors(); // 一个错误记录器

    public MariaDBTableGenerator(String tableName, Randomly r, MariaDBSchema newSchema) {
        this.tableName = tableName;
        this.s = newSchema;
        this.r = r;
    }

    /*
    这是一个静态工厂方法，是外部调用这个类的标准方式。它简化了调用过程：你只需要提供表名、随机数生成器和数据库状态，它就会返回一个完整的 SQLQueryAdapter 结果。
     */
    public static SQLQueryAdapter generate(String tableName, Randomly r, MariaDBSchema newSchema) {
        return new MariaDBTableGenerator(tableName, r, newSchema).gen();
    }

    private SQLQueryAdapter gen() {
        if (Randomly.getBoolean() || s.getDatabaseTables().isEmpty()) { // 从零开始，详细地、一列一列地构建一个全新的表。如果数据库里本来就一张表都没有，也必须走这条路。
            newTable();
        } else {
            likeOtherTable(); // 使用 CREATE TABLE ... LIKE ... 语法，复制一个数据库中已经存在的表的结构。这是一种相对简单的建表方式
        }
        return new SQLQueryAdapter(sb.toString(), errors, true); // 把 sb 中构建好的 SQL 字符串和 errors 对象包装成 SQLQueryAdapter 并返回
    }

    private enum PrimaryKeyState {
        NO_PRIMARY_KEY, COLUMN_CONSTRAINT, TABLE_CONSTRAINT
    }

//    private void newTable() {
//        createOrReplaceTable();
//        sb.append("(");
//        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
//            if (i != 0) {
//                sb.append(", ");
//            }
//            String columnName = DBMSCommon.createColumnName(i);
//            columnNames.add(columnName);
//            sb.append(columnName);
//            sb.append(" ");
//            MariaDBDataType dataType = Randomly.fromOptions(MariaDBDataType.values());
//            switch (dataType) {
//            case INT:
//                sb.append(Randomly.fromOptions("SMALLINT", "MEDIUMINT", "INT", "BIGINT"));
//                addSignedness();
//                break;
//            case VARCHAR:
//                sb.append(Randomly.fromOptions("VARCHAR(100)", "CHAR(100)"));
//                break;
//            case REAL:
//                sb.append("REAL");
//                addSignedness();
//                break;
//            case BOOLEAN:
//                sb.append("BOOLEAN");
//                break;
//            default:
//                throw new AssertionError(dataType);
//            }
//            final boolean isGeneratedColumn;
//            if (Randomly.getBoolean() && !MariaDBBugs.bug21058) {
//                sb.append(" GENERATED ALWAYS AS(");
//                // TODO columns
//                sb.append(MariaDBVisitor.asString(new MariaDBExpressionGenerator(r).getRandomExpression()));
//                sb.append(")");
//                isGeneratedColumn = true;
//            } else {
//                isGeneratedColumn = false;
//            }
//            sb.append(" ");
//            if (Randomly.getBoolean() && !isGeneratedColumn) {
//                sb.append(" UNIQUE");
//            }
//            if (Randomly.getBoolean() && primaryKeyState == PrimaryKeyState.COLUMN_CONSTRAINT && !isGeneratedColumn) {
//                sb.append(" PRIMARY KEY");
//                primaryKeyState = PrimaryKeyState.NO_PRIMARY_KEY;
//            }
//            if (Randomly.getBoolean() && !isGeneratedColumn) {
//                sb.append(" NOT NULL");
//            }
//        }
//        if (primaryKeyState == PrimaryKeyState.TABLE_CONSTRAINT) {
//            sb.append(", PRIMARY KEY(");
//            sb.append(Randomly.nonEmptySubset(columnNames).stream().collect(Collectors.joining(", ")));
//            sb.append(")");
//            errors.add("Primary key cannot be defined upon a generated column");
//
//        }
//        sb.append(")");
//        if (Randomly.getBoolean()) {
//            sb.append(" engine=");
//            sb.append(MariaDBEngine.getRandomEngine().getTextRepresentation());
//        }
//    }
    private void newTable() {
        /* 这个辅助方法负责构建语句的开头部分，比如 CREATE TABLE t0 或 CREATE OR REPLACE TABLE IF NOT EXISTS t0。它也随机决定是否添加 OR REPLACE 和 IF NOT EXISTS 这些可选子句。 */
        createOrReplaceTable();
        sb.append("(");
        // 循环的次数是随机的（Randomly.smallNumber() 通常返回一个较小的随机数），意味着表的列数也是随机的
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String columnName = DBMSCommon.createColumnName(i); // DBMSCommon.createColumnName(i) 生成一个标准化的列名，如 c0, c1, c2
            columnNames.add(columnName);
            sb.append(columnName);
            sb.append(" ");
            MariaDBDataType dataType = Randomly.fromOptions(MariaDBDataType.values()); // 通过 Randomly.fromOptions(MariaDBDataType.values()) 随机选择一个基本数据类型
            switch (dataType) {
                //进一步随机选择是哪种INT，并且通过 addSignedness() 随机决定是否带符号 (SIGNED/UNSIGNED/ZEROFILL)
                case INT:
                    sb.append(Randomly.fromOptions("SMALLINT", "MEDIUMINT", "INT", "BIGINT"));
                    addSignedness();
                    break;
                case VARCHAR:
                    sb.append(Randomly.fromOptions("VARCHAR(100)", "CHAR(100)"));
                    break;
                case REAL:
                    sb.append("REAL");
                    addSignedness();
                    break;
                case BOOLEAN:
                    sb.append("BOOLEAN");
                    break;
                default:
                    throw new AssertionError(dataType);
            }
            final boolean isGeneratedColumn;
            if (Randomly.getBoolean() && !MariaDBBugs.bug21058) {
                sb.append(" GENERATED ALWAYS AS(");
                // TODO columns
                sb.append(MariaDBVisitor.asString(new MariaDBExpressionGenerator(r).getRandomExpression()));
                sb.append(")");
                isGeneratedColumn = true;
            } else {
                isGeneratedColumn = false;
            }
            sb.append(" ");
            if (Randomly.getBoolean() && !isGeneratedColumn) {
                sb.append(" UNIQUE");
            }
            // 如果 primaryKeyState 被设定为 COLUMN_CONSTRAINT，就在这里为第一个合适的非生成列添加 PRIMARY KEY 约束，并把状态重置，确保主键只被创建一次
            if (Randomly.getBoolean() && primaryKeyState == PrimaryKeyState.COLUMN_CONSTRAINT && !isGeneratedColumn) {
                sb.append(" PRIMARY KEY");
                primaryKeyState = PrimaryKeyState.NO_PRIMARY_KEY;
            }

            // ========================= START OF MODIFICATION =========================

            // 移除了 Randomly.getBoolean()，强制为所有非生成列添加 NOT NULL。
            if (!isGeneratedColumn) {
                sb.append(" NOT NULL");
            }

            // ========================== END OF MODIFICATION ==========================
        }
        if (primaryKeyState == PrimaryKeyState.TABLE_CONSTRAINT) {
            sb.append(", PRIMARY KEY(");
            sb.append(Randomly.nonEmptySubset(columnNames).stream().collect(Collectors.joining(", ")));
            sb.append(")");
            errors.add("Primary key cannot be defined upon a generated column");

        }
        sb.append(")");
        if (Randomly.getBoolean()) {
            // 最后，随机决定是否为表指定一个存储引擎（如 InnoDB, MyISAM, Aria 等）。
            sb.append(" engine=");
            sb.append(MariaDBEngine.getRandomEngine().getTextRepresentation());
        }
    }

    private void addSignedness() {
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("SIGNED", "UNSIGNED", "ZEROFILL"));
        }
    }

    /*这个方法非常简单。它调用 createOrReplaceTable() 生成语句开头，然后从 s (数据库状态) 中随机找一个已经存在的表，并追加 LIKE existing_table_name*/
    private void likeOtherTable() {
        createOrReplaceTable(); // CREATE TABLE tableName ...
        sb.append(" LIKE ");
        sb.append(s.getRandomTable().getName());
    }

    private void createOrReplaceTable() {
        sb.append("CREATE ");
        boolean replace = false;
        if (Randomly.getBoolean()) {
            sb.append("OR REPLACE ");
            replace = true;
        }
        // TODO temporary
        // if (Randomly.getBoolean()) {
        // sb.append("TEMPORARY ");
        // }
        sb.append("TABLE ");
        if (Randomly.getBoolean() && !replace) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(tableName);
        errors.add("Specified key was too long; max key length is");
    }

}
