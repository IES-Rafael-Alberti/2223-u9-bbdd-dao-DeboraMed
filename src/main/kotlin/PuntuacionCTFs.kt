import java.sql.DriverManager
import java.sql.SQLException
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.pair
import com.github.ajalt.clikt.parameters.options.triple
import com.github.ajalt.clikt.parameters.types.int


data class Ctf(val id: Int, val grupoId: Int, val puntuacion: Int)
data class Grupo(val grupoid: Int, val mejorCtfId: Int = 0)


class PuntuacionCTFs :CliktCommand(){

    //val formatType: String by option("-a", "--format", help = "Tipo de formato").default("ts")
    val opcionAnadeParticipacion by option("-a").int().triple()
    val opcionEliminaParticipacion by option("-d").int().pair()
    val opcionMuestraParticipacion by option("-l").int()


    val jdbcUrl = "jdbc:h2:./db" // ruta relativa
    val userName = "sa"
    val password = "sa"

    override fun run() {

        if(opcionAnadeParticipacion!=null) {
            val id = opcionAnadeParticipacion!!.first
            val grupoId = opcionAnadeParticipacion!!.second
            val puntuacion = opcionAnadeParticipacion!!.third
            var ctf = Ctf(id, grupoId, puntuacion)
            anadeParticipacion(ctf)
        }
        else if(opcionEliminaParticipacion!=null) {
            val ctfid = opcionEliminaParticipacion!!.first
            val grupoId = opcionEliminaParticipacion!!.second
            eliminaParticipacion(ctfid,grupoId)
        }
        else if(opcionMuestraParticipacion!=null) {
            val grupoId = opcionMuestraParticipacion!!
            muestraParticipacion(grupoId)
        }
        else
            println("Se requiere una opcion valida")

        /*
        //mi main
        var ctf=Ctf(3,3,342344)
        anadeParticipacion(ctf)

        val grupoid=3
        val ctfid=3
        eliminaParticipacion(grupoid,ctfid)

        var grupoid1=1
        muestraParticipacion(grupoid1)*/
    }



    // 1º comando añade participacion de un grupo (insert en ctf) consulta sobre ctf con mejor puntucion de ese grupo
    //(update) de mejor posicion
    fun anadeParticipacion(ctf:Ctf){

        try {

            //conexion con la BBDD
            val conn = DriverManager.getConnection(jdbcUrl, userName, password)
            //sentencia insert
            val insertSqlString = ("INSERT INTO CTFS(CTFid, grupoid, puntuacion) VALUES(?,?,?)")
            //dado un equipo, el ID del CTF donde hizo mas puntos


            var preparedStatement = conn.prepareStatement(insertSqlString) // Uso de PreparedStatement

            preparedStatement.setInt(1, ctf.id)
            preparedStatement.setInt(2, ctf.grupoId)
            preparedStatement.setInt(3, ctf.puntuacion)

            //ejecuta el insert
            val insertCount = preparedStatement.executeUpdate()


            val updateSqlString = ("UPDATE GRUPOS\n" +
                    "SET mejorposCTFid = (\n" +
                    "  select ctfid\n" +
                    "  from ctfs\n" +
                    "  where grupoid=? and puntuacion=(\n" +
                    "    select max(puntuacion)\n" +
                    "    from CTFS\n" +
                    "    where grupoid=?\n" +
                    "  )\n" +
                    "  limit 1\n" +
                    ")\n" +
                    "where grupoid=?;")


            preparedStatement = conn.prepareStatement(updateSqlString) // Uso de PreparedStatement

            preparedStatement.setInt(1, ctf.grupoId)
            preparedStatement.setInt(2, ctf.grupoId)
            preparedStatement.setInt(3, ctf.grupoId)

            //ejecuta el update
            preparedStatement.executeUpdate()



            //comprueba si se ha realizado el insert
            if (insertCount > 0) {
                println("Procesado: Añadida participación del grupo ${ctf.grupoId} en el ${ctf.id} con una puntuacion de ${ctf.puntuacion} puntos.")
            } else {
                println("No procesado: La inserción de fila ha fallado.")
            }

            preparedStatement.close()

        }catch (ex: SQLException) {
            println(ex.message);

        }


    }

    // 2º comando elimina participacion de un grupo (insert en ctf) consulta sobre ctf con mejor puntucion de ese grupo
    //(update) de mejor posicion

    fun eliminaParticipacion(ctfid:Int,grupoid: Int){

        try{
            //conexion con la BBDD
            val conn = DriverManager.getConnection(jdbcUrl, userName, password)

            //actualiza el campo que es clave foranea y no not null a null
            var updateStatement = ("UPDATE GRUPOS SET mejorposCTFid=NULL where grupoid=?;")

            var preparedStatement = conn.prepareStatement(updateStatement) // Uso de PreparedStatement

            preparedStatement.setInt(1, grupoid)

            //ejecuta el delete
            preparedStatement.executeUpdate()


            //ELIMINA LA PARTICIPACION DE UN GRUPO EN EL ctf
            val deleteSqlString = ("DELETE FROM CTFS WHERE grupoid=? AND ctfid=?")

            preparedStatement = conn.prepareStatement(deleteSqlString) // Uso de PreparedStatement

            preparedStatement.setInt(1, grupoid)
            preparedStatement.setInt(2, ctfid)


            //ejecuta el delete y cuenta cuantos
            val deleteCount = preparedStatement.executeUpdate()

            //Y RECALCULA EL CAMPO MEJORPOS DE LOS GRUPOS EN LA TABLA GRUPOS
            val updateSqlString = ("UPDATE GRUPOS\n" +
                    "SET mejorposCTFid = (\n" +
                    "  select ctfid\n" +
                    "  from ctfs\n" +
                    "  where grupoid=? and puntuacion=(\n" +
                    "    select max(puntuacion)\n" +
                    "    from CTFS\n" +
                    "    where grupoid=?\n" +
                    "  )\n" +
                    "  limit 1\n" +
                    ")\n" +
                    "where grupoid=?;")


            preparedStatement = conn.prepareStatement(updateSqlString) // Uso de PreparedStatement

            preparedStatement.setInt(1, grupoid)
            preparedStatement.setInt(2, grupoid)
            preparedStatement.setInt(3, grupoid)

            //ejecuta el update
            preparedStatement.executeUpdate()


            //comprueba si se ha realizado el delete
            if (deleteCount > 0) {
                println("Procesado: Eliminada participación del grupo $grupoid en el CTF $ctfid.")
            } else {
                println("No procesado: La eliminación de la fila ha fallado.")
            }
            preparedStatement.close()

        }catch (ex: SQLException) {
            println(ex.message);

        }

    }


    // 3º select del grupo dado, sino info de todo
    fun muestraParticipacion(grupoid:Int){

        try{
            //conexion con la BBDD
            val conn = DriverManager.getConnection(jdbcUrl, userName, password)


            //actualiza el campo que es clave foranea y no not null a null
            var psSelectGroup = conn.prepareStatement("select * from grupos where grupoid=?") // Uso de PreparedStatement

            psSelectGroup.setInt(1, grupoid)

            //ejecuta el delete
            var resultSelectGroup = psSelectGroup.executeQuery()



            var resultadoVacio = true
            //compruebo el resultado de la consulta
            while (resultSelectGroup.next()) {
                resultadoVacio = false
                val grupoid = resultSelectGroup.getString("grupoid")
                val grupodesc  = resultSelectGroup.getString("grupodesc")
                val mejorposCTFid = resultSelectGroup.getString("mejorposCTFid")
                println("GRUPO: $grupoid  $grupodesc  MEJORCTF: $mejorposCTFid")
            }

            if(resultadoVacio) {

                var sSelectAllGroup = conn.createStatement() // Uso de PreparedStatement

                //ejecuta el delete
                var resultSelectAllGroup = sSelectAllGroup.executeQuery("select * from grupos")

                while (resultSelectAllGroup.next()) {
                    val grupoid = resultSelectAllGroup.getString("grupoid")
                    val grupodesc  = resultSelectAllGroup.getString("grupodesc")
                    val mejorposCTFid = resultSelectAllGroup.getString("mejorposCTFid")
                    println("GRUPO: $grupoid  $grupodesc  MEJORCTF: $mejorposCTFid")
                }

            }
            psSelectGroup.close()


        }catch (ex: SQLException) {
            println(ex.message);

        }


    }



}