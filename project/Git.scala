import com.typesafe.sbt.GitPlugin.autoImport.git

object Git {

  git.baseVersion := Versions.sparkOracleVersion

  val VersionRegex = "v([0-9]+.[0-9]+.[0-9]+)-?(.*)?".r

  git.gitTagToVersionNumber := {
    case VersionRegex(v,"") => Some(v)
    case VersionRegex(v,"SNAPSHOT") => Some(s"$v-SNAPSHOT")
    case VersionRegex(v,s) => Some(s"$v-$s-SNAPSHOT")
    case _ => None
  }

}
