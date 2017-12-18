package es.arjon

/*import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp*/

case class Flight(FechaHora: String,
                  Callsign: String,
                  Matricula: String,
                  Aeronave_Cod: String,
                  Aerolinea_Cod: String,
                  Aerolinea_Nombre: String,
                  Origen: String,
                  Destino: String)

object Flight {
  def fromTXT(flight: String, line: String): Option[Flight] = {
    val v = line.split(";")

    try
      Some {
        Flight(
           Callsign = v(1).toString,
          FechaHora = v(0).toString,
          Aeronave_Cod = v(3).toString,
            Aerolinea_Cod=v(4).toString,
          Matricula = v(2).toString,
          Aerolinea_Nombre = v(4).toString,
          Origen = v(5).toString,
          Destino = v(6).toString
        )
      }

    catch {
      case ex: Exception => {
        println(s"Failed to process $flight, with input $line, with ${ex.toString}")
        None
      }
    }

  }
}