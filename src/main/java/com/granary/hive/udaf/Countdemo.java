/**
 * 
 */
package com.granary.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * @author yuanpeng.song Create on 2018年9月27日
 */
public class Countdemo extends AbstractGenericUDAFResolver {

	/**
	 * 对如参数据进行判断、校验
	 */
	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		if (parameters.length != 1) {
			throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
		}
		ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
		if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Argument must be PRIMITIVE, but " + oi.getCategory().name() + " was passed.");
		}
		PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;
		if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentTypeException(0,
					"Argument must be String, but " + inputOI.getPrimitiveCategory().name() + " was passed.");
		}
		return new Evaluator();
	}

	public static class Evaluator extends GenericUDAFEvaluator {
		PrimitiveObjectInspector inputOI;
		ObjectInspector outputOI;
		PrimitiveObjectInspector integerOI;

		int total = 0;

		/**
		 * 确定各个阶段输入输出参数的数据格式ObjectInspectors
		 */
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			assert (parameters.length == 1);
			super.init(m, parameters);
			// mapi阶段输入的为string类型
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				inputOI = (PrimitiveObjectInspector) parameters[0];
			} else {
				// 其余阶段输入均为integer类型
				integerOI = (PrimitiveObjectInspector) parameters[0];
			}
			// 指定各个阶段输出的均为integer
			outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorOptions.JAVA);
			return outputOI;
		}

		/**
		 * AggregationBuffer 允许我们保存中间结果，通过定义我们的buffer，
		 * 我们可以处理任何格式的数据，在代码例子中字符总数保存在AggregationBuffer
		 */
		static class Countagg implements AggregationBuffer {
			int count = 0;

			void add(int num) {
				count += num;
			}
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			Countagg agg = new Countagg();
			return agg;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			assert (parameters.length == 1);
			if (parameters[0] != null) {
				Countagg a = (Countagg) agg;
				// 总个数增加一个
				a.add(1);
			}
		}

		/**
		 * combiner合并map返回的结果， 还有reducer合并mapper或combiner返回的结果
		 * 
		 * Merge函数增加部分聚集总数到AggregationBuffer
		 * 
		 * @param agg
		 * @param partial
		 * @throws HiveException
		 */
		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			if (partial != null) {
				Countagg a_01 = (Countagg) agg;
				Integer partialNum = (Integer) integerOI.getPrimitiveJavaObject(partial);
				Countagg a_02 = new Countagg();

				a_02.add(partialNum);
				a_01.add(a_02.count);

			}
		}

		/**
		 * 重置聚合结果集，如果想复用这个数据集，这个函数是必要的
		 * 
		 * @param agg
		 * @throws HiveException
		 */
		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			Countagg countagg = new Countagg();
		}

		/**
		 * Terminate()函数返回AggregationBuffer中的内容，这里产生了最终结果。 最终结果
		 * 
		 * @param agg
		 * @return
		 * @throws HiveException
		 */
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			Countagg a_01 = (Countagg) agg;
			total = a_01.count;
			// return total;
			return a_01.count;
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
			Countagg count =(Countagg) agg;
			total += count.count;
			return total;
		}

	}

}
