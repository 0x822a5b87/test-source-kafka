readme
编译
MavenDeployment
使用 gradle 6.2 可以直接编译，更高版本的部分插件被移除。

ScalaCompileOptions.metaClass.useAnt ...
在 build.gradle 最上方添加

ScalaCompileOptions.metaClass.daemonServer = true
ScalaCompileOptions.metaClass.fork = true
ScalaCompileOptions.metaClass.useAnt = false
ScalaCompileOptions.metaClass.useCompileDaemon = false
allowInsecureProtocol
gradle 要求用户使用私有库时显示的声明，所以在 buildscript.gradle 中增加配置

repositories {
  repositories {
    // For license plugin.
    maven {
      url = 'http://dl.bintray.com/content/netflixoss/external-gradle-plugins/'
      allowInsecureProtocol = true
    }
  }
}
