package org.hkust.parser;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

        String expected = "{\n" +
                "  \"AggregateProcessFunction\": [\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"orderkey\",\n" +
                "        \"o_orderdate\",\n" +
                "        \"o_shippriority\"\n" +
                "      ],\n" +
                "      \"AggregateValue\": [\n" +
                "        {\n" +
                "          \"value_type\": \"double\",\n" +
                "          \"name\": \"revenue\",\n" +
                "          \"aggregation\": \"+\",\n" +
                "          \"value\": {\n" +
                "            \"right_field\": {\n" +
                "              \"right_field\": {\n" +
                "                \"name\": \"l_discount\",\n" +
                "                \"type\": \"attribute\",\n" +
                "                \"relation\": \"lineitem\"\n" +
                "              },\n" +
                "              \"left_field\": {\n" +
                "                \"var_type\": \"int\",\n" +
                "                \"type\": \"constant\",\n" +
                "                \"value\": 1\n" +
                "              },\n" +
                "              \"type\": \"expression\",\n" +
                "              \"operator\": \"-\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_extendedprice\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"*\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"output_key\": [\n" +
                "        \"orderkey\",\n" +
                "        \"o_orderdate\",\n" +
                "        \"o_shippriority\"\n" +
                "      ],\n" +
                "      \"name\": \"QAggregate0\",\n" +
                "      \"delta_output\": true\n" +
                "    }\n" +
                "  ],\n" +
                "  \"join_structure\": [\n" +
                "    {\n" +
                "      \"primary\": \"orders\",\n" +
                "      \"foreign\": \"lineitem\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"primary\": \"customer\",\n" +
                "      \"foreign\": \"orders\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"RelationProcessFunction\": [\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"orderkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"select_conditions\": {\n" +
                "        \"values\": [\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"Date\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"1995-03-15\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_shipdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \">\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"name\": \"l_commitdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_receiptdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \">\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"operator\": \"&&\"\n" +
                "      },\n" +
                "      \"name\": \"Qlineitem\",\n" +
                "      \"next_key\": [\n" +
                "        \"orderkey\",\n" +
                "        \"o_orderdate\",\n" +
                "        \"o_shippriority\"\n" +
                "      ],\n" +
                "      \"is_Root\": true,\n" +
                "      \"child_nodes\": 1,\n" +
                "      \"relation\": \"lineitem\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"select_conditions\": {\n" +
                "        \"values\": [\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"Date\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"1995-03-15\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"o_orderdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"orders\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"<\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"operator\": \"&&\"\n" +
                "      },\n" +
                "      \"name\": \"Qorders\",\n" +
                "      \"next_key\": [\n" +
                "        \"orderkey\"\n" +
                "      ],\n" +
                "      \"is_Root\": false,\n" +
                "      \"child_nodes\": 1,\n" +
                "      \"relation\": \"orders\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"select_conditions\": {\n" +
                "        \"values\": [\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"varchar\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"BUILDING\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"c_mktsegment\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"customer\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"==\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"operator\": \"&&\"\n" +
                "      },\n" +
                "      \"name\": \"Qcustomer\",\n" +
                "      \"next_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Root\": false,\n" +
                "      \"child_nodes\": 0,\n" +
                "      \"relation\": \"customer\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SQLParser = new Parser(sql, "test.json");

        SQLParser.parse();

        String actual= FileUtils.readFileToString(new File("test.json"),"UTF-8");
        assertEquals(expected, actual);
    }
}
