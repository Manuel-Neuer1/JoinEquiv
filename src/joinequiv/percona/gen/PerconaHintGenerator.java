package joinequiv.percona.gen;

import joinequiv.IgnoreMeException;
import joinequiv.Randomly;
import joinequiv.percona.PerconaSchema.PerconaIndex;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.ast.PerconaSelect;
import joinequiv.percona.ast.PerconaText;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PerconaHintGenerator {

    private final PerconaSelect select;
    private final List<PerconaTable> tables;
    private final StringBuilder sb = new StringBuilder();

    enum OptimizeHint {
        BKA, NO_BKA, BNL, NO_BNL, DERIVED_CONDITION_PUSHDOWN, NO_DERIVED_CONDITION_PUSHDOWN, GROUP_INDEX,
        NO_GROUP_INDEX, HASH_JOIN, NO_HASH_JOIN, INDEX, NO_INDEX, INDEX_MERGE, NO_INDEX_MERGE, JOIN_FIXED_ORDER,
        JOIN_INDEX, NO_JOIN_INDEX, JOIN_ORDER, JOIN_PREFIX, JOIN_SUFFIX, MERGE, NO_MERGE, MRR, NO_MRR, NO_ICP,
        NO_RANGE_OPTIMIZATION, ORDER_INDEX, NO_ORDER_INDEX, SEMIJOIN, NO_SEMIJOIN, SKIP_SCAN, NO_SKIP_SCAN
    }

    public PerconaHintGenerator(PerconaSelect select, List<PerconaTable> tables) {
        this.select = select;
        this.tables = tables;
    }

    public static void generateHints(PerconaSelect select, List<PerconaTable> tables) {
        new PerconaHintGenerator(select, tables).randomHint();
    }

    public static List<PerconaText> generateAllHints(PerconaSelect select, List<PerconaTable> tables) {
        PerconaHintGenerator generator = new PerconaHintGenerator(select, tables);
        return generator.allHints();
    }

    private void randomHint() {
        OptimizeHint chosenhint = Randomly.fromOptions(OptimizeHint.values());
        generate(chosenhint);
    }

    private List<PerconaText> allHints() {
        List<PerconaText> results = new ArrayList<>();
        for (OptimizeHint hint : OptimizeHint.values()) {
            try {
                PerconaText generatedHint = generate(hint);
                results.add(generatedHint);
            } catch (IgnoreMeException e) {
                continue;
            }
        }
        return results;
    }

    private PerconaText generate(OptimizeHint chosenhint) {
        sb.setLength(0);

        switch (chosenhint) {
        case BKA:
            tablesHint("BKA");
            break;
        case NO_BKA:
            tablesHint("NO_BKA");
            break;
        case BNL:
            tablesHint("BNL");
            break;
        case NO_BNL:
            tablesHint("NO_BNL");
            break;
        case DERIVED_CONDITION_PUSHDOWN:
            tablesHint("DERIVED_CONDITION_PUSHDOWN");
            break;
        case NO_DERIVED_CONDITION_PUSHDOWN:
            tablesHint("NO_DERIVED_CONDITION_PUSHDOWN");
            break;
        case GROUP_INDEX:
            indexesHint("GROUP_INDEX");
            break;
        case NO_GROUP_INDEX:
            indexesHint("NO_GROUP_INDEX");
            break;
        case HASH_JOIN:
            tablesHint("HASH_JOIN");
            break;
        case NO_HASH_JOIN:
            tablesHint("NO_HASH_JOIN");
            break;
        case INDEX:
            indexesHint("INDEX");
            break;
        case NO_INDEX:
            indexesHint("NO_INDEX");
            break;
        case INDEX_MERGE:
            indexesHint("INDEX_MERGE");
            break;
        case NO_INDEX_MERGE:
            indexesHint("NO_INDEX_MERGE");
            break;
        case JOIN_FIXED_ORDER:
            tablesHint("JOIN_FIXED_ORDER");
            break;
        case JOIN_INDEX:
            indexesHint("JOIN_INDEX");
            break;
        case NO_JOIN_INDEX:
            indexesHint("NO_JOIN_INDEX");
            break;
        case JOIN_ORDER:
            tablesHint("JOIN_ORDER");
            break;
        case JOIN_PREFIX:
            tablesHint("JOIN_PREFIX");
            break;
        case JOIN_SUFFIX:
            tablesHint("JOIN_SUFFIX");
            break;
        case MERGE:
            tablesHint("MERGE");
            break;
        case NO_MERGE:
            tablesHint("NO_MERGE");
            break;
        case MRR:
            indexesHint("MRR");
            break;
        case NO_MRR:
            indexesHint("NO_MRR");
            break;
        case NO_ICP:
            indexesHint("NO_ICP");
            break;
        case NO_RANGE_OPTIMIZATION:
            indexesHint("NO_RANGE_OPTIMIZATION");
            break;
        case ORDER_INDEX:
            indexesHint("ORDER_INDEX");
            break;
        case NO_ORDER_INDEX:
            indexesHint("NO_ORDER_INDEX");
            break;
        case SEMIJOIN:
            semiHint("SEMIJOIN");
            break;
        case NO_SEMIJOIN:
            semiHint("NO_SEMIJOIN");
            break;
        case SKIP_SCAN:
            indexesHint("SKIP_SCAN");
            break;
        case NO_SKIP_SCAN:
            indexesHint("NO_SKIP_SCAN");
            break;
        default:
            throw new AssertionError();
        }
        PerconaText hint = new PerconaText(sb.toString());
        select.setHint(hint);
        return hint;
    }

    private void indexesHint(String string) {
        sb.append(string);
        sb.append("(");
        PerconaTable table = Randomly.fromList(tables);
        List<PerconaIndex> allIndexes = table.getIndexes();
        sb.append(table.getName());
        sb.append(", ");
        if (allIndexes.isEmpty()) {
            sb.append("PRIMARY");
        } else {
            List<PerconaIndex> indexSubset = Randomly.nonEmptySubset(allIndexes);
            sb.append(indexSubset.stream().map(i -> i.getIndexName()).distinct().collect(Collectors.joining(", ")));
        }
        sb.append(")");
    }

    private void tablesHint(String string) {
        sb.append(string);
        sb.append("(");
        appendTables();
        sb.append(")");
    }

    private void semiHint(String string) {
        sb.append(string);
        sb.append("(");
        String[] options = { "DUPSWEEDOUT", "FIRSTMATCH", "LOOSESCAN", "MATERIALIZATION" };
        List<String> chosenOptions = Randomly.nonEmptySubset(options);
        sb.append(chosenOptions.stream().collect(Collectors.joining(", ")));
        sb.append(")");
    }

    private void appendTables() {
        List<PerconaTable> tableSubset = Randomly.nonEmptySubset(tables);
        sb.append(tableSubset.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
    }

}
