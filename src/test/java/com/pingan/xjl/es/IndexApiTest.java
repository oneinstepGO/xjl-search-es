package com.pingan.xjl.es;

import com.pingan.xjl.es.api.IndexApi;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.io.IOException;

import static com.pingan.xjl.es.constant.EsConstants.BOOK_INDEX;

/**
 * @author Aaron
 * @date 2020/5/3 1:20
 */

public class IndexApiTest extends XjlSearchEsApplicationTests {

    @Autowired
    private IndexApi indexApi;

    /**
     * 创建索引
     * @throws IOException
     */
    @Test
    public void testCreateIndex() throws IOException {

        String mappingJsonStr =   "{\n" +
                "      \"properties\":{\n" +
                "        \"bookId\":{\n" +
                "          \"type\":\"text\",\n" +
                "          \"analyzer\": \"keyword\"\n" +
                "        },\n" +
                "        \"title\":{\n" +
                "          \"type\":\"text\",\n" +
                "          \"analyzer\": \"ik_max_word\"\n" +
                "        },\n" +
                "         \"author\":{\n" +
                "          \"type\":\"keyword\"\n" +
                "        },\n" +
                "          \"summary\":{\n" +
                "          \"type\":\"text\",\n" +
                "          \"analyzer\": \"ik_max_word\"\n" +
                "        },\n" +
                "         \"price\":{\n" +
                "          \"type\":\"double\"\n" +
                "        },\n" +
                "        \"publishDate\":{\n" +
                "          \"type\":\"date\"\n" +
                "        },\n" +
                "        \"numReviews\":{\n" +
                "          \"type\":\"integer\"\n" +
                "        },\n" +
                "          \"publisher\":{\n" +
                "          \"type\":\"text\",\n" +
                "          \"analyzer\": \"ik_max_word\"\n" +
                "        }\n" +
                "      }\n" +
                "    }";

        Assert.isTrue(indexApi.create(BOOK_INDEX,1,1,mappingJsonStr));
    }

    /**
     * 删除索引
     */
    @Test
    public void testDelete() {
        Assert.isTrue(indexApi.delete(BOOK_INDEX));
    }
}
