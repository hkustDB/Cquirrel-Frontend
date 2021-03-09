package org.hkust.parser;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SQLParserTest {
    private Parser SQLParser;
    private String sql;

    @Test
    public void SQLParserQ3Test() throws Exception {
        sql = "select\n" +
                "l_orderkey, \n" +
                "sum(l_extendedprice*(1-l_discount)) as revenue,\n" +
                "o_orderdate, \n" +
                "o_shippriority\n" +
                "from \n" +
                "customer c, \n" +
                "orders o, \n" +
                "lineitem l\n" +
                "where \n" +
                "c_mktsegment = 'BUILDING'\n" +
                "and c_custkey=o_custkey\n" +
                "and l_orderkey=o_orderkey\n" +
                "and o_orderdate < date '1995-03-15'\n" +
                "and l_shipdate > date '1995-03-15'\n" +
                "and l_receiptdate > l_commitdate \n" +
                "group by \n" +
                "l_orderkey, \n" +
                "o_orderdate, \n" +
                "o_shippriority;";

        SQLParser = new Parser(sql, "test.json");

        SQLParser.parse();
    }
}
