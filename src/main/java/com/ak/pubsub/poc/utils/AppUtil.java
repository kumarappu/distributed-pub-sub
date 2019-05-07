package com.ak.pubsub.poc.utils;

import com.ak.pubsub.poc.proto.domain.CommonProto;

import java.math.BigDecimal;
import java.util.Random;

/**
 * Created by appu_kumar on 5/1/2019.
 */
public class AppUtil {

    private static final ThreadLocal<CommonProto.Instrument.Builder> instrumentBuilderThreadLocal
            = ThreadLocal.withInitial(() -> CommonProto.Instrument.newBuilder());

    private static final ThreadLocal<CommonProto.Instrument.Field.Builder> fieldBuilderThreadLocal
            = ThreadLocal.withInitial(() -> CommonProto.Instrument.Field.newBuilder());

    private static final ThreadLocal<CommonProto.Instrument.Field.FieldValue.Builder> fieldValueBuilderThreadLocal
            = ThreadLocal.withInitial(() -> CommonProto.Instrument.Field.FieldValue.newBuilder());

    Random random = new Random();

    public CommonProto.Instrument  createMockUpdate(String identifier){
        CommonProto.Instrument.Builder instrumentBuilder = instrumentBuilderThreadLocal.get();
        instrumentBuilder.clear();

        instrumentBuilder.setIdentifier(identifier);

        populateFieldValue(instrumentBuilder,1,System.currentTimeMillis(),CommonProto.Instrument.FieldType.LONG);
        populateFieldValue(instrumentBuilder,2,random.nextInt(1000),CommonProto.Instrument.FieldType.INT);
        populateFieldValue(instrumentBuilder,3,random.nextInt(1000),CommonProto.Instrument.FieldType.INT);
        populateFieldValue(instrumentBuilder,4,random.nextInt(1000),CommonProto.Instrument.FieldType.INT);
        populateFieldValue(instrumentBuilder,5,random.nextInt(1000),CommonProto.Instrument.FieldType.INT);
        populateFieldValue(instrumentBuilder,6,random.nextInt(1000),CommonProto.Instrument.FieldType.INT);
        populateFieldValue(instrumentBuilder,7,random.nextInt(1000),CommonProto.Instrument.FieldType.INT);
        populateFieldValue(instrumentBuilder,8,random.nextInt(1000),CommonProto.Instrument.FieldType.INT);
        populateFieldValue(instrumentBuilder,9,random.nextInt(1000),CommonProto.Instrument.FieldType.INT);
        populateFieldValue(instrumentBuilder,10,random.nextInt(1000),CommonProto.Instrument.FieldType.INT);

        return instrumentBuilder.build();
    }



    public void populateFieldValue(CommonProto.Instrument.Builder instrumentBuilder,
                                int  fieldId, Object fieldValue, CommonProto.Instrument.FieldType fieldType) {
        if (fieldValue == null) {
            return;
        }

        CommonProto.Instrument.Field.Builder fieldBuilder = fieldBuilderThreadLocal.get();
        fieldBuilder.clear();

        CommonProto.Instrument.Field.FieldValue.Builder fieldValueBuilder = fieldValueBuilderThreadLocal.get();
        fieldValueBuilder.clear();


        switch (fieldType) {
            case STRING:
                String stringVal = ((String) fieldValue).trim();
                fieldValueBuilder.setStringVal(stringVal);
                break;
            case INT:
                if(fieldValue instanceof  Integer){
                    fieldValueBuilder.setInt32Val((int) fieldValue);
                }else if(fieldValue instanceof  Short){
                    fieldValueBuilder.setInt32Val((short) fieldValue);
                }else if(fieldValue instanceof  Byte){
                    fieldValueBuilder.setInt32Val((byte) fieldValue);
                }
                break;
            case DOUBLE:
                if(fieldValue instanceof  Double){
                    fieldValueBuilder.setDoubleVal((double) fieldValue);
                }else if(fieldValue instanceof  BigDecimal){
                    fieldValueBuilder.setDoubleVal(((BigDecimal) fieldValue).doubleValue());
                }

                break;
            case LONG:
                fieldValueBuilder.setInt64Val((long) fieldValue);
                break;
            case BOOLEAN:
                fieldValueBuilder.setBoolVal((boolean) fieldValue);
                break;
            case LIST:
                stringVal = ((String) fieldValue).trim();
                fieldValueBuilder.setStringVal(stringVal);
                break;
            default:
                return;
        }

        fieldValueBuilder.setFieldValueType(fieldType);

        fieldBuilder.setFieldId(fieldId);
        fieldBuilder.setFieldType(fieldType);
        fieldBuilder.addFieldValue(fieldValueBuilder.build());

        instrumentBuilder.addFields(fieldBuilder.build());
    }





}
