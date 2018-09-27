package com.granary.hive.udaf;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.IOException;

/**
 * @author yuanpeng.song
 * @create 2018/9/27
 * @since 1.0.0
 */
public class TotalNumOfLettersGenericUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        }
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Argument must be PRIMITIVE, but " + oi.getCategory().name() + " was passed.");
        }
        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "Argument must be String, but " + inputOI.getPrimitiveCategory().name() + " was passed.");
        }
        return new TotalNumOfLettersEvaluator();
    }

    public static class TotalNumOfLettersEvaluator extends GenericUDAFEvaluator {
        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;
        PrimitiveObjectInspector integerOI;

        int total = 0;

        public TotalNumOfLettersEvaluator() {
            super();
        }

        @Override
        public void configure(MapredContext mapredContext) {
            super.configure(mapredContext);
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
            // init方法中根据不同的mode指定输出数据的格式objectinspector
            //map阶段读取sql列，输入为String基础数据格式
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                //其余阶段，输入为Integer基础数据格式
                integerOI = (PrimitiveObjectInspector) parameters[0];
            }
            // 指定各个阶段输出数据格式都为Integer类型
            outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return outputOI;


        }

        /**
         * AggregationBuffer 允许我们保存中间结果，通过定义我们的buffer，
         * 我们可以处理任何格式的数据，在代码例子中字符总数保存在AggregationBuffer
         */
        static class Sumagg implements AggregationBuffer {
            int sum = 0;

            void add(int num) {
                sum += num;
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
        }

        @Override
        public void aggregate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            super.aggregate(agg, parameters);
        }

        @Override
        public Object evaluate(AggregationBuffer agg) throws HiveException {
            return super.evaluate(agg);
        }

        @Override
        public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
            return super.getWindowingEvaluator(wFrmDef);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            Sumagg res = new Sumagg();
            return res;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            Sumagg aggs = new Sumagg();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] != null) {
                Sumagg s = (Sumagg) agg;
                Object p1 = ((PrimitiveObjectInspector) inputOI).getPrimitiveJavaObject(parameters[0]);
                s.add(String.valueOf(p1).length());
            }
        }

        /**
         * map和combiner结束返回结果，得到部分数据聚集结果
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            Sumagg s = (Sumagg) agg;
            total += s.sum;
            return total;
        }

        /**
         * combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果
         * Merge函数增加部分聚集总数到AggregationBuffer
         *
         * @param agg
         * @param partial
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                Sumagg myagg1 = (Sumagg) agg;
                Integer partialSum = (Integer) integerOI.getPrimitiveJavaObject(partial);
                Sumagg myagg2 = new Sumagg();
                myagg2.add(partialSum);
                myagg1.add(myagg2.sum);


            }
        }

        /**
         * Terminate()函数返回AggregationBuffer中的内容，这里产生了最终结果。
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            Sumagg myagg = (Sumagg) agg;
            total = myagg.sum;
            return myagg.sum;
        }
    }
}