package com.jw9j.flink.mysql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author jw9j
 * @create 2021/7/6 23:24
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AzureBillingDetail {
    private String Id;
    private String ProviderId;
    private String TenantId;
    private String AccountOwnerId;
    private String AccountName;
    private String ServiceAdministratorId;
    private String SubscriptionId;
    private String SubscriptionGuid;
    private String SubscriptionName;
    private String Date;
    private String Month;
    private String Day;
    private String Year;
    private String Product;
    private String MeterId;
    private String MeterCategory;
    private String MeterSubCategory;
    private String MeterRegion;
    private String MeterName;
    private String ConsumedQuantity;
    private String ResourceRate;
    private String ExtendedCost;
    private String ResourceLocation;
    private String ConsumedService;
    private String InstanceId;
    private String ServiceInfo1;
    private String ServiceInfo2;
    private String AdditionalInfo;
    private String Tags;
    private String StoreServiceIdentifier;
    private String DepartmentName;
    private String CostCenter;
    private String UnitofMeasure;
    private String ResourceGroup;
    private String CreateTime;
}
