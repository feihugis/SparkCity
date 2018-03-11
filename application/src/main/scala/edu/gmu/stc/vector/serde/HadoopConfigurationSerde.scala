package edu.gmu.stc.vector.serde

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import edu.gmu.stc.config.ConfigParameter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark.internal.Logging

/**
  * Created by Fei Hu on 1/29/18.
  */
class HadoopConfigurationSerde extends Serializer with Logging{
  override def read(kryo: Kryo, input: Input, aClass: Class[Nothing]): Nothing = {
    val configuration = new Configuration()
    configuration.set(ConfigParameter.HIBERNATE_DRIEVER, input.readString())
    configuration.set(ConfigParameter.HIBERNATE_URL, input.readString())
    configuration.set(ConfigParameter.HIBERNATE_USER, input.readString())
    configuration.set(ConfigParameter.HIBERNATE_PASS, input.readString())
    configuration.set(ConfigParameter.HIBERNATE_DIALECT, input.readString())
    configuration.set(ConfigParameter.HIBERNATE_HBM2DDL_AUTO, input.readString())

    configuration.set(ConfigParameter.INPUT_DIR_PATH, input.readString())

    configuration.asInstanceOf[Nothing]
  }

  override def write(kryo: Kryo, output: Output, t: Nothing): Unit = {
    if (t.isInstanceOf[Configuration]) {
      val config = t.asInstanceOf[Configuration]
      output.writeString(config.get(ConfigParameter.HIBERNATE_DRIEVER))
      output.writeString(config.get(ConfigParameter.HIBERNATE_URL))
      output.writeString(config.get(ConfigParameter.HIBERNATE_USER))
      output.writeString(config.get(ConfigParameter.HIBERNATE_PASS))
      output.writeString(config.get(ConfigParameter.HIBERNATE_DIALECT))
      output.writeString(config.get(ConfigParameter.HIBERNATE_HBM2DDL_AUTO))

      output.writeString(config.get(ConfigParameter.INPUT_DIR_PATH))
    } else {
      logError("Does not support the serialization for this class " + t.getClass.toString)
    }
  }
}
