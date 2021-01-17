package org.tikv.tiflink.server.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.tikv.tiflink.server.service.Executor;

@RestController
public class Proxy {
  private static Logger logger = LoggerFactory.getLogger(Proxy.class);
  @Autowired private Executor executor;

  @PostMapping(value = "/", consumes = "application/json")
  @ResponseBody
  public String execute(@RequestBody String json) throws Exception {
    return executeCreateMView(json);
  }

  @GetMapping(value = "/debug")
  @ResponseBody
  public String executeDebug() throws Exception {
    String json = "{\"id\":0,\"name\":{\"O\":\"v\",\"L\":\"v\"},\"charset\":\"utf8\",\"collate\":\"utf8_general_ci\",\"cols\":[{\"id\":1,\"name\":{\"O\":\"id\",\"L\":\"id\"},\"offset\":0,\"origin_default\":null,\"origin_default_bit\":null,\"default\":null,\"default_bit\":null,\"default_is_expr\":false,\"generated_expr_string\":\"\",\"generated_stored\":false,\"dependences\":null,\"type\":{\"Tp\":0,\"Flag\":0,\"Flen\":0,\"Decimal\":0,\"Charset\":\"\",\"Collate\":\"\",\"Elems\":null},\"state\":5,\"comment\":\"\",\"hidden\":false,\"version\":0},{\"id\":2,\"name\":{\"O\":\"first_name\",\"L\":\"first_name\"},\"offset\":1,\"origin_default\":null,\"origin_default_bit\":null,\"default\":null,\"default_bit\":null,\"default_is_expr\":false,\"generated_expr_string\":\"\",\"generated_stored\":false,\"dependences\":null,\"type\":{\"Tp\":0,\"Flag\":0,\"Flen\":0,\"Decimal\":0,\"Charset\":\"\",\"Collate\":\"\",\"Elems\":null},\"state\":5,\"comment\":\"\",\"hidden\":false,\"version\":0},{\"id\":3,\"name\":{\"O\":\"last_name\",\"L\":\"last_name\"},\"offset\":2,\"origin_default\":null,\"origin_default_bit\":null,\"default\":null,\"default_bit\":null,\"default_is_expr\":false,\"generated_expr_string\":\"\",\"generated_stored\":false,\"dependences\":null,\"type\":{\"Tp\":0,\"Flag\":0,\"Flen\":0,\"Decimal\":0,\"Charset\":\"\",\"Collate\":\"\",\"Elems\":null},\"state\":5,\"comment\":\"\",\"hidden\":false,\"version\":0},{\"id\":4,\"name\":{\"O\":\"email\",\"L\":\"email\"},\"offset\":3,\"origin_default\":null,\"origin_default_bit\":null,\"default\":null,\"default_bit\":null,\"default_is_expr\":false,\"generated_expr_string\":\"\",\"generated_stored\":false,\"dependences\":null,\"type\":{\"Tp\":0,\"Flag\":0,\"Flen\":0,\"Decimal\":0,\"Charset\":\"\",\"Collate\":\"\",\"Elems\":null},\"state\":5,\"comment\":\"\",\"hidden\":false,\"version\":0},{\"id\":5,\"name\":{\"O\":\"posts\",\"L\":\"posts\"},\"offset\":4,\"origin_default\":null,\"origin_default_bit\":null,\"default\":null,\"default_bit\":null,\"default_is_expr\":false,\"generated_expr_string\":\"\",\"generated_stored\":false,\"dependences\":null,\"type\":{\"Tp\":0,\"Flag\":0,\"Flen\":0,\"Decimal\":0,\"Charset\":\"\",\"Collate\":\"\",\"Elems\":null},\"state\":5,\"comment\":\"\",\"hidden\":false,\"version\":0}],\"index_info\":null,\"fk_info\":null,\"state\":0,\"pk_is_handle\":false,\"comment\":\"\",\"auto_inc_id\":0,\"auto_id_cache\":0,\"auto_rand_id\":0,\"max_col_id\":5,\"max_idx_id\":0,\"update_timestamp\":0,\"ShardRowIDBits\":0,\"max_shard_row_id_bits\":0,\"auto_random_bits\":0,\"pre_split_regions\":0,\"partition\":null,\"compression\":\"\",\"view\":{\"view_algorithm\":3,\"view_definer\":{\"Username\":\"root\",\"Hostname\":\"127.0.0.1\",\"CurrentUser\":false,\"AuthUsername\":\"root\",\"AuthHostname\":\"%\"},\"view_security\":0,\"view_select\":\"SELECT `id`,`first_name`,`last_name`,`email`,(SELECT COUNT(1) FROM `test`.`posts` WHERE `author_id`=`authors`.`id`) AS `posts` FROM `test`.`authors`\",\"view_checkoption\":1,\"view_cols\":null},\"sequence\":null,\"Lock\":null,\"version\":3,\"tiflash_replica\":null}";

    return executeCreateMView(json);
  }

  @PostMapping(value = "/create_materialized_views", consumes = "application/json")
  @ResponseBody
  public String executeCreateMViews(@RequestBody String json) throws Exception {
    return executeCreateMView(json);
  }

  @PostMapping(value = "/create_materialized_view", consumes = "application/json")
  @ResponseBody
  public String executeCreateMView(@RequestBody String json) throws Exception {
    logger.info("execute got param {}", json);
    String result;
    JSONObject single = JSON.parseObject(json);
    String selectSql = single.getJSONObject("view").getString("view_select");
    String tableName = single.getJSONObject("name").getString("L");
    logger.info("execute : {}", selectSql);
    result = executor.execute(selectSql, tableName);

    return result;
  }
}
