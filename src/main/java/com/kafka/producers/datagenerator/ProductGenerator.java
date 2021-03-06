package com.kafka.producers.datagenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.model.LineItem;
import com.kafka.producers.common.ProducerConstants;

import java.io.File;
import java.util.Random;

class ProductGenerator {
    private static final ProductGenerator ourInstance = new ProductGenerator();
    private final Random random;
    private final Random qty;
    private final LineItem[] products;

    static ProductGenerator getInstance() {
        return ourInstance;
    }

    private ProductGenerator() {
        String DATAFILE = ProducerConstants.PRODUCTS_JSON_FILE;
        ObjectMapper mapper = new ObjectMapper();
        random = new Random();
        qty = new Random();
        try {
            products = mapper.readValue(new File(DATAFILE), LineItem[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int getIndex() {
        return random.nextInt(100);
    }

    private int getQuantity() {
        return qty.nextInt(2) + 1;
    }

    LineItem getNextProduct() {
        LineItem lineItem = products[getIndex()];
        lineItem.setItemQty(getQuantity());
        lineItem.setTotalValue(lineItem.getItemPrice() * lineItem.getItemQty());
        return lineItem;
    }
}
