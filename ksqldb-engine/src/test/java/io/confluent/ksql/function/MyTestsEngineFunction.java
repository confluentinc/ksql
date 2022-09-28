package io.confluent.ksql.function;

import io.confluent.ksql.function.udaf.array.CollectListUdafTest;
import io.confluent.ksql.function.udaf.array.CollectSetUdafTest;
import io.confluent.ksql.function.udaf.attr.AttrTest;
import io.confluent.ksql.function.udaf.average.AverageUdafTest;
import io.confluent.ksql.function.udaf.correlation.CorrelationUdafTest;
import io.confluent.ksql.function.udaf.count.CountDistinctKudafTest;
import io.confluent.ksql.function.udaf.count.CountKudafTest;
import io.confluent.ksql.function.udaf.map.HistogramUdafTest;
import io.confluent.ksql.function.udaf.max.BytesMaxKudafTest;
import io.confluent.ksql.function.udaf.max.DateMaxKudafTest;
import io.confluent.ksql.function.udaf.max.DecimalMaxKudafTest;
import io.confluent.ksql.function.udaf.max.DoubleMaxKudafTest;
import io.confluent.ksql.function.udaf.max.IntegerMaxKudafTest;
import io.confluent.ksql.function.udaf.max.LongMaxKudafTest;
import io.confluent.ksql.function.udaf.max.StringMaxKudafTest;
import io.confluent.ksql.function.udaf.max.TimeMaxKudafTest;
import io.confluent.ksql.function.udaf.max.TimestampMaxKudafTest;
import io.confluent.ksql.function.udaf.min.BytesMinKudafTest;
import io.confluent.ksql.function.udaf.min.DateMinKudafTest;
import io.confluent.ksql.function.udaf.min.DecimalMinKudafTest;
import io.confluent.ksql.function.udaf.min.DoubleMinKudafTest;
import io.confluent.ksql.function.udaf.min.IntegerMinKudafTest;
import io.confluent.ksql.function.udaf.min.LongMinKudafTest;
import io.confluent.ksql.function.udaf.min.StringMinKudafTest;
import io.confluent.ksql.function.udaf.min.TimeMinKudafTest;
import io.confluent.ksql.function.udaf.min.TimestampMinKudafTest;
import io.confluent.ksql.function.udaf.offset.EarliestByOffsetTest;
import io.confluent.ksql.function.udaf.stddev.StandardDeviationSampUdafTest;
import io.confluent.ksql.function.udaf.stddev.StandardDeviationSampleUdafTest;
import io.confluent.ksql.function.udaf.sum.BaseSumKudafTest;
import io.confluent.ksql.function.udaf.sum.DecimalSumKudafTest;
import io.confluent.ksql.function.udaf.sum.DoubleSumKudafTest;
import io.confluent.ksql.function.udaf.sum.IntegerSumKudafTest;
import io.confluent.ksql.function.udaf.sum.ListSumUdafTest;
import io.confluent.ksql.function.udaf.sum.LongSumKudafTest;
import io.confluent.ksql.function.udaf.topk.DoubleTopkKudafTest;
import io.confluent.ksql.function.udaf.topk.IntTopkKudafTest;
import io.confluent.ksql.function.udaf.topk.LongTopkKudafTest;
import io.confluent.ksql.function.udaf.topk.StringTopkKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.BytesTopKDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.DateTopKDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.DecimalTopKDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.DoubleTopkDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.IntTopkDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.LongTopkDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.StringTopkDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.TimeTopKDistinctKudafTest;
import io.confluent.ksql.function.udaf.topkdistinct.TimestampTopKDistinctKudafTest;
import io.confluent.ksql.function.udf.AsValueTest;
import io.confluent.ksql.function.udf.array.ArrayConcatTest;
import io.confluent.ksql.function.udf.array.ArrayDistinctTest;
import io.confluent.ksql.function.udf.array.ArrayExceptTest;
import io.confluent.ksql.function.udf.array.ArrayIntersectTest;
import io.confluent.ksql.function.udf.array.ArrayJoinTest;
import io.confluent.ksql.function.udf.array.ArrayLengthTest;
import io.confluent.ksql.function.udf.array.ArrayMaxTest;
import io.confluent.ksql.function.udf.array.ArrayMinTest;
import io.confluent.ksql.function.udf.array.ArrayRemoveTest;
import io.confluent.ksql.function.udf.array.ArraySortTest;
import io.confluent.ksql.function.udf.array.ArrayUnionTest;
import io.confluent.ksql.function.udf.array.EntriesTest;
import io.confluent.ksql.function.udf.array.GenerateSeriesTest;
import io.confluent.ksql.function.udf.conversions.BigIntFromBytesTest;
import io.confluent.ksql.function.udf.conversions.DoubleFromBytesTest;
import io.confluent.ksql.function.udf.conversions.IntFromBytesTest;
import io.confluent.ksql.function.udf.datetime.ConvertTzTest;
import io.confluent.ksql.function.udf.datetime.DateAddTest;
import io.confluent.ksql.function.udf.datetime.DateSubTest;
import io.confluent.ksql.function.udf.datetime.DateToStringTest;
import io.confluent.ksql.function.udf.datetime.FormatDateTest;
import io.confluent.ksql.function.udf.datetime.FormatTimeTest;
import io.confluent.ksql.function.udf.datetime.FormatTimestampTest;
import io.confluent.ksql.function.udf.datetime.FromDaysTest;
import io.confluent.ksql.function.udf.datetime.FromUnixTimeTest;
import io.confluent.ksql.function.udf.datetime.ParseDateTest;
import io.confluent.ksql.function.udf.datetime.ParseTimeTest;
import io.confluent.ksql.function.udf.datetime.ParseTimestampTest;
import io.confluent.ksql.function.udf.datetime.StringToDateTest;
import io.confluent.ksql.function.udf.datetime.StringToTimestampTest;
import io.confluent.ksql.function.udf.datetime.TimeAddTest;
import io.confluent.ksql.function.udf.datetime.TimeSubTest;
import io.confluent.ksql.function.udf.datetime.TimestampAddTest;
import io.confluent.ksql.function.udf.datetime.TimestampSubTest;
import io.confluent.ksql.function.udf.datetime.TimestampToStringTest;
import io.confluent.ksql.function.udf.datetime.UnixDateTest;
import io.confluent.ksql.function.udf.datetime.UnixTimestampTest;
import io.confluent.ksql.function.udf.geo.GeoDistanceTest;
import io.confluent.ksql.function.udf.json.IsJsonStringTest;
import io.confluent.ksql.function.udf.json.JsonArrayContainsTest;
import io.confluent.ksql.function.udf.json.JsonArrayLengthTest;
import io.confluent.ksql.function.udf.json.JsonConcatTest;
import io.confluent.ksql.function.udf.json.JsonExtractStringKudfTest;
import io.confluent.ksql.function.udf.json.JsonItemsTest;
import io.confluent.ksql.function.udf.json.JsonKeysTest;
import io.confluent.ksql.function.udf.json.JsonRecordsTest;
import io.confluent.ksql.function.udf.json.ToJsonStringTest;
import io.confluent.ksql.function.udf.lambda.FilterTest;
import io.confluent.ksql.function.udf.lambda.ReduceTest;
import io.confluent.ksql.function.udf.lambda.TransformTest;
import io.confluent.ksql.function.udf.list.ArrayContainsTest;
import io.confluent.ksql.function.udf.list.SliceTest;
import io.confluent.ksql.function.udf.map.AsMapTest;
import io.confluent.ksql.function.udf.map.MapKeysTest;
import io.confluent.ksql.function.udf.map.MapUnionTest;
import io.confluent.ksql.function.udf.map.MapValuesTest;
import io.confluent.ksql.function.udf.math.AbsTest;
import io.confluent.ksql.function.udf.math.AcosTest;
import io.confluent.ksql.function.udf.math.AsinTest;
import io.confluent.ksql.function.udf.math.Atan2Test;
import io.confluent.ksql.function.udf.math.AtanTest;
import io.confluent.ksql.function.udf.math.CbrtTest;
import io.confluent.ksql.function.udf.math.CosTest;
import io.confluent.ksql.function.udf.math.CoshTest;
import io.confluent.ksql.function.udf.math.CotTest;
import io.confluent.ksql.function.udf.math.DegreesTest;
import io.confluent.ksql.function.udf.math.ExpTest;
import io.confluent.ksql.function.udf.math.GreatestTest;
import io.confluent.ksql.function.udf.math.LeastTest;
import io.confluent.ksql.function.udf.math.LnTest;
import io.confluent.ksql.function.udf.math.LogTest;
import io.confluent.ksql.function.udf.math.PiTest;
import io.confluent.ksql.function.udf.math.PowerTest;
import io.confluent.ksql.function.udf.math.RadiansTest;
import io.confluent.ksql.function.udf.math.RandomTest;
import io.confluent.ksql.function.udf.math.RoundTest;
import io.confluent.ksql.function.udf.math.SignTest;
import io.confluent.ksql.function.udf.math.SinTest;
import io.confluent.ksql.function.udf.math.SinhTest;
import io.confluent.ksql.function.udf.math.SqrtTest;
import io.confluent.ksql.function.udf.math.TanTest;
import io.confluent.ksql.function.udf.math.TanhTest;
import io.confluent.ksql.function.udf.math.TruncTest;
import io.confluent.ksql.function.udf.nulls.CoalesceTest;
import io.confluent.ksql.function.udf.nulls.NullIfTest;
import io.confluent.ksql.function.udf.string.ChrTest;
import io.confluent.ksql.function.udf.string.ConcatTest;
import io.confluent.ksql.function.udf.string.ConcatWSTest;
import io.confluent.ksql.function.udf.string.EltTest;
import io.confluent.ksql.function.udf.string.EncodeTest;
import io.confluent.ksql.function.udf.string.FieldTest;
import io.confluent.ksql.function.udf.string.FromBytesTest;
import io.confluent.ksql.function.udf.string.InitCapTest;
import io.confluent.ksql.function.udf.string.InstrTest;
import io.confluent.ksql.function.udf.string.LCaseTest;
import io.confluent.ksql.function.udf.string.LPadTest;
import io.confluent.ksql.function.udf.string.LenTest;
import io.confluent.ksql.function.udf.string.MaskKeepLeftTest;
import io.confluent.ksql.function.udf.string.MaskKeepRightTest;
import io.confluent.ksql.function.udf.string.MaskLeftTest;
import io.confluent.ksql.function.udf.string.MaskRightTest;
import io.confluent.ksql.function.udf.string.MaskTest;
import io.confluent.ksql.function.udf.string.MaskerTest;
import io.confluent.ksql.function.udf.string.RPadTest;
import io.confluent.ksql.function.udf.string.RegexpExtractAllTest;
import io.confluent.ksql.function.udf.string.RegexpExtractTest;
import io.confluent.ksql.function.udf.string.RegexpReplaceTest;
import io.confluent.ksql.function.udf.string.RegexpSplitToArrayTest;
import io.confluent.ksql.function.udf.string.ReplaceTest;
import io.confluent.ksql.function.udf.string.SplitTest;
import io.confluent.ksql.function.udf.string.SplitToMapTest;
import io.confluent.ksql.function.udf.string.SubstringTest;
import io.confluent.ksql.function.udf.string.ToBytesTest;
import io.confluent.ksql.function.udf.string.TrimTest;
import io.confluent.ksql.function.udf.string.UCaseTest;
import io.confluent.ksql.function.udf.string.UuidTest;
import io.confluent.ksql.function.udf.url.UrlDecodeParamTest;
import io.confluent.ksql.function.udf.url.UrlEncodeParamTest;
import io.confluent.ksql.function.udf.url.UrlExtractFragmentTest;
import io.confluent.ksql.function.udf.url.UrlExtractHostTest;
import io.confluent.ksql.function.udf.url.UrlExtractParameterTest;
import io.confluent.ksql.function.udf.url.UrlExtractPathTest;
import io.confluent.ksql.function.udf.url.UrlExtractPortTest;
import io.confluent.ksql.function.udf.url.UrlExtractProtocolTest;
import io.confluent.ksql.function.udf.url.UrlExtractQueryTest;
import io.confluent.ksql.function.udtf.CubeTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        CollectListUdafTest.class,
        CollectSetUdafTest.class,
        AttrTest.class,
        AverageUdafTest.class,
        CorrelationUdafTest.class,
        CountDistinctKudafTest.class,
        CountKudafTest.class,
        HistogramUdafTest.class,
        BytesMaxKudafTest.class,
        DateMaxKudafTest.class,
        DecimalMaxKudafTest.class,
        DoubleMaxKudafTest.class,
        IntegerMaxKudafTest.class,
        LongMaxKudafTest.class,
        StringMaxKudafTest.class,
        TimeMaxKudafTest.class,
        TimestampMaxKudafTest.class,
        BytesMinKudafTest.class,
        DateMinKudafTest.class,
        DecimalMinKudafTest.class,
        DoubleMinKudafTest.class,
        IntegerMinKudafTest.class,
        LongMinKudafTest.class,
        StringMinKudafTest.class,
        TimeMinKudafTest.class,
        TimestampMinKudafTest.class,
        EarliestByOffsetTest.class,
        StandardDeviationSampleUdafTest.class,
        StandardDeviationSampUdafTest.class,
        BaseSumKudafTest.class,
        DecimalSumKudafTest.class,
        DoubleSumKudafTest.class,
        DoubleSumKudafTest.class,
        IntegerSumKudafTest.class,
        ListSumUdafTest.class,
        LongSumKudafTest.class,
        DoubleTopkKudafTest.class,
        IntTopkKudafTest.class,
        LongTopkKudafTest.class,
        StringTopkKudafTest.class,
        BytesTopKDistinctKudafTest.class,
        DateTopKDistinctKudafTest.class,
        DecimalTopKDistinctKudafTest.class,
        DoubleTopkDistinctKudafTest.class,
        IntTopkDistinctKudafTest.class,
        LongTopkDistinctKudafTest.class,
        StringTopkDistinctKudafTest.class,
        TimestampTopKDistinctKudafTest.class,
        TimeTopKDistinctKudafTest.class,
        ArrayConcatTest.class,
        ArrayDistinctTest.class,
        ArrayExceptTest.class,
        ArrayIntersectTest.class,
        ArrayJoinTest.class,
        ArrayLengthTest.class,
        ArrayMaxTest.class,
        ArrayMinTest.class,
        ArrayRemoveTest.class,
        ArraySortTest.class,
        ArrayUnionTest.class,
        EntriesTest.class,
        GenerateSeriesTest.class,
        BigIntFromBytesTest.class,
        DoubleFromBytesTest.class,
        IntFromBytesTest.class,
        ConvertTzTest.class,
        DateAddTest.class,
        DateSubTest.class,
        DateToStringTest.class,
        FormatDateTest.class,
        FormatTimestampTest.class,
        FormatTimeTest.class,
        FromDaysTest.class,
        FromUnixTimeTest.class,
        ParseDateTest.class,
        ParseTimestampTest.class,
        ParseTimeTest.class,
        StringToDateTest.class,
        StringToTimestampTest.class,
        TimeAddTest.class,
        TimestampAddTest.class,
        TimestampSubTest.class,
        TimestampToStringTest.class,
        TimeSubTest.class,
        UnixDateTest.class,
        UnixTimestampTest.class,
        GeoDistanceTest.class,
        IsJsonStringTest.class,
        JsonArrayContainsTest.class,
        JsonArrayLengthTest.class,
        JsonConcatTest.class,
        JsonExtractStringKudfTest.class,
        JsonItemsTest.class,
        JsonKeysTest.class,
        JsonRecordsTest.class,
        ToJsonStringTest.class,
        FilterTest.class,
        ReduceTest.class,
        TransformTest.class,
        ArrayContainsTest.class,
        SliceTest.class,
        AsMapTest.class,
        MapKeysTest.class,
        MapUnionTest.class,
        MapValuesTest.class,
        AbsTest.class,
        AcosTest.class,
        AsinTest.class,
        Atan2Test.class,
        AtanTest.class,
        CbrtTest.class,
        CoshTest.class,
        CosTest.class,
        CotTest.class,
        DegreesTest.class,
        ExpTest.class,
        GreatestTest.class,
        LeastTest.class,
        LnTest.class,
        LogTest.class,
        PiTest.class,
        PowerTest.class,
        RadiansTest.class,
        RandomTest.class,
        RoundTest.class,
        SignTest.class,
        SinhTest.class,
        SinTest.class,
        SqrtTest.class,
        TanhTest.class,
        TanTest.class,
        TruncTest.class,
        CoalesceTest.class,
        NullIfTest.class,
        ChrTest.class,
        ConcatTest.class,
        ConcatWSTest.class,
        EltTest.class,
        EncodeTest.class,
        FieldTest.class,
        FromBytesTest.class,
        InitCapTest.class,
        InstrTest.class,
        LCaseTest.class,
        LenTest.class,
        LPadTest.class,
        MaskerTest.class,
        MaskKeepLeftTest.class,
        MaskKeepRightTest.class,
        MaskLeftTest.class,
        MaskRightTest.class,
        MaskTest.class,
        RegexpExtractAllTest.class,
        RegexpExtractTest.class,
        RegexpReplaceTest.class,
        RegexpSplitToArrayTest.class,
        ReplaceTest.class,
        RPadTest.class,
        SplitTest.class,
        SplitToMapTest.class,
        SubstringTest.class,
        ToBytesTest.class,
        TrimTest.class,
        UCaseTest.class,
        UuidTest.class,
        UrlDecodeParamTest.class,
        UrlEncodeParamTest.class,
        UrlExtractFragmentTest.class,
        UrlExtractHostTest.class,
        UrlExtractParameterTest.class,
        UrlExtractPathTest.class,
        UrlExtractPortTest.class,
        UrlExtractProtocolTest.class,
        UrlExtractQueryTest.class,
        AsValueTest.class,
        CubeTest.class,
        BaseAggregateFunctionTest.class,
        BlacklistTest.class,
        FunctionMetricsTest.class,
        InternalFunctionRegistryTest.class,
        UdafAggregateFunctionFactoryTest.class,
        UdafTypesTest.class,
        UdfClassLoaderTest.class,
//        UdfLoaderTest.class,
        UdfMetricProducerTest.class,
//        UdtfLoaderTest.class,
})
public class MyTestsEngineFunction {
}
