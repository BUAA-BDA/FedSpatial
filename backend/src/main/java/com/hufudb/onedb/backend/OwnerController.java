package com.hufudb.onedb.backend;

import java.util.List;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.data.schema.utils.PojoTableSchema;
import com.hufudb.onedb.owner.OwnerService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(
    name = {"owner.db.enable"},
    havingValue = "true")
public class OwnerController {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerController.class);
  private final OwnerService service;

  OwnerController(OwnerService service) {
    this.service = service;
  }

  @GetMapping("/owner/localtables")
  List<PojoTableSchema> getLocalTableInfos() {
    return PojoTableSchema.from(service.getAllLocalTable());
  }

  @GetMapping("/owner/publishedtables")
  List<PojoPublishedTableSchema> getPublishedTableInfos() {
    return PojoPublishedTableSchema.from(service.getAllPublishedTable());
  }

  @PostMapping("/owner/publishedtables")
  boolean addPublishedTable(@RequestBody PojoPublishedTableSchema schema) {
    return service.addPublishedTable(schema);
  }

  @DeleteMapping("/owner/publishedtables/{name}")
  void dropPublishedTable(@PathVariable String name) {
    service.dropPublishedTable(name);
  }
}
