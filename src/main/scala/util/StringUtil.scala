package util

import java.util.regex.Pattern

import scala.collection.mutable.ArrayBuffer

object StringUtil {
  val regex = "[1-9]\\d+"
  val sb = new StringBuffer()
  val pattern: Pattern = Pattern.compile(regex)

  def cleanSentence(text: String): String = {
    text.replaceAll(
      """\\p{:!),.:;?.]}¢'"
        |  、 。 〉 》 」 』 〕 〗 〞 ︰ ︱ ︳
        |  ﹐ ､ ﹒ ﹔ ﹕ ﹖ ﹗ ﹚ ﹜ ﹞ ！ ） ， ． ： ； ？ ｜
        |  ｝ ︴
        |  ︶ ︸ ︺ ︼ ︾ ﹀ ﹂ ﹄ ﹏
        |  ､ ￠々
        |  ‖ • · ˇˉ
        |  ― ′ ’ ” ([ {£¥ ' "‵〈《「『〔〖（［｛￡￥〝︵︷︹︻︽︿﹁﹃﹙﹛﹝（｛“‘_…/}""".stripMargin, "")
  }

  def cleanName(name: String): String = {
    if ("".equals(name)) {
      ""
    }
    else {
      name.toLowerCase().trim().replace(".", "")
        .replace("-", "")
        .replace(" ", "_")
    }
  }

  def cleanOrg(_org: String): String = {
    var org = _org
    if (!"".equals(org)) {
      org = org.toLowerCase()
      org = org.replace("sch.", "school")
      org = org.replace("dept.", "department")
      org = org.replace("coll.", "college")
      org = org.replace("inst.", "institute")
      org = org.replace("univ.", "university")
      org = org.replace("lab ", "laboratory ")
      org = org.replace("univ ", "university ")
      org = org.replace("lab.", "laboratory")
      org = org.replace("natl.", "national")
      org = org.replace("comp.", "computer")
      org = org.replace("sci.", "science")
      org = org.replace("tech.", "technology")
      org = org.replace("technol.", "technology")
      org = org.replace("elec.", "electronic")
      org = org.replace("engr.", "engineering")
      org = org.replace("aca.", "academy")
      org = org.replace("syst.", "systems")
      org = org.replace("eng.", "engineering")
      org = org.replace("res.", "research")
      org = org.replace("appl.", "applied")
      org = org.replace("chem.", "chemistry")
      org = org.replace("prep.", "petrochemical")
      org = org.replace("phys.", "physics")
      org = org.replace("phys ", "physics")
      org = org.replace("mech.", "mechanics")
      org = org.replace("mat.", "material")
      org = org.replace("cent.", "center")
      org = org.replace("ctr.", "center")
      org = org.replace("behav.", "behavior")
      org = org.replace("atom.", "atomic")
      org = org.replace("~", " ")
      org = org.replace("#", " ")
      org = org.replace("od", "of")
      org = org.replace("%", " ")
      org = org.replace("^", " ")
      org = org.replace("&", " ")
      org = org.replace("(", " ")
      org = org.replace(")", " ")
      org = org.replace("et al.", " ")
      org = org.replace(";", ",")
      org = org.replace("/", " ")
      org = org.replace(".", " ")
      org = org.replace(",", " ")
      val matcher = pattern.matcher(org)
      org = matcher.replaceAll(" ")
      //      if (matcher.find()){
      //        matcher.appendReplacement(sb, " ")
      //        org = sb.toString.replace("-", " ").replace(",", " ")
      //      }
      val parts = org.trim().split(" ")
      val orgWordList = ArrayBuffer[String]()
      for (part <- parts) {
        if (part.contains("@") || part.equals("") || part.equals(" ")) {
        } else {
          orgWordList.append(part)
        }
      }
      orgWordList.mkString(" ")
    }
    else {
      ""
    }
  }

  def main(args: Array[String]): Unit = {
    val org = "1234Tsing4354656hua 43343 Univ."
    println(org)
    println(cleanOrg(org))
  }

}
