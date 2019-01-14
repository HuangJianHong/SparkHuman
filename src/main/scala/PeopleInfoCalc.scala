package src.main.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分析数据：求：男女的总人数、最高（低）身高  3 F 177
  */
object PeopleInfoCalc {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PeopleInfoCalculate").setMaster("local")
    val sc = new SparkContext(conf)

    val dataFile = sc.textFile("D:\\temp\\sample_people_info.txt", 2)

    //拆分：男、女
    val maleData: RDD[String] = dataFile.filter(line => line.contains("M")).map(line => {
      line.split(" ")(1) + " " + line.split(" ")(2)
    })

    val FemaleData: RDD[String] = dataFile.filter(line => line.contains("F")).map(line => {
      line.split(" ")(1) + " " + line.split(" ")(2)
    })

    //得到各自的身高
    val maleHeightData = maleData.map(line => line.split(" ")(1).toInt)
    val femaleHeightData = FemaleData.map(line => line.split(" ")(1).toInt)

    //最高和最低值
    val lowestMale: Int = maleHeightData.sortBy(x => x, true).first()
    val lowestFemale = femaleHeightData.sortBy(x => x, true).first()

    val highestMale = maleHeightData.sortBy(x => x, false).first()
    val highestFemale = femaleHeightData.sortBy(x => x, false).first()


    println("Number of Male Peole:" + maleData.count())
    println("Number of Female Peole:" + FemaleData.count())
    println("Lowest Male:" + lowestMale)
    println("Lowest Female:" + lowestFemale)
    println("Highest Male:" + highestMale)
    println("Highest Female:" + highestFemale)

    sc.stop();

    /**
      * 输出结果：
      * Number of Male Peole:490
      * Number of Female Peole:510
      * Lowest Male:100
      * Lowest Female:100
      * Highest Male:219
      * Highest Female:219
      */
  }


}
