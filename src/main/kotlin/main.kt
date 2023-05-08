import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

val jdbcUrl = "jdbc:h2:./db" // ruta relativa
val userName = "sa"
val password = "sa"



data class Ctf(val id: Int, val grupoId: Int, val puntuacion: Int)
data class Grupo(val grupoid: Int, val mejorCtfId: Int = 0)

fun main(args: Array<String>) {
    /*conexion con la BBDD
    val conn = DriverManager.getConnection(jdbcUrl, userName, password) // Uso de Connection

    val stmt: Statement = conn.createStatement()
    val rs: ResultSet = stmt.executeQuery("SELECT grupoid FROM GRUPOS")

    while (rs.next()) {
        val lastName = rs.getString("grupoid")
        println(lastName)
    }


    //codigo que venia
    val participaciones = listOf(Ctf(1, 1, 3), Ctf(1, 2, 101), Ctf(2, 2, 3), Ctf(2, 1, 50), Ctf(2, 3, 1), Ctf(3, 1, 50), Ctf(3, 3, 5))
    val mejoresCtfByGroupId = calculaMejoresResultados(participaciones)
    println(mejoresCtfByGroupId)
     */

    //mi codigo
    var ctf=Ctf(3,3,342344)
    anadeParticipacion(ctf)

    val grupoid=3
    val ctfid=3
    eliminaParticipacion(grupoid,ctfid)

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

fun eliminaParticipacion(grupoid: Int,ctfid:Int){

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

    //conexion con la BBDD
    val conn = DriverManager.getConnection(jdbcUrl, userName, password)


    val stmt: Statement = conn.createStatement()
    val rs: ResultSet = stmt.executeQuery("select * from grupos where grupoid=?;")

    if (rs.wasNull()) {
        //poner condicion de que se imprima entera
        val lastName = rs.getString("grupoid")
        println(lastName)
    }



    //select del grupo dado
    var updateStatement = ("select grupoid,grupodesc,mejorposCTFid\n" +
                            "from grupos\n" +
                            "where grupoid =?;")

    var preparedStatement = conn.prepareStatement(updateStatement) // Uso de PreparedStatement

    preparedStatement.setInt(1, grupoid)

    //ejecuta el select
    preparedStatement.executeUpdate()



}










/**
 * TODO
 *
 * @param participaciones
 * @return devuelve un mutableMapOf<Int, Pair<Int, Ctf>> donde
 *      Key: el grupoId del grupo
 *      Pair:
 *          first: Mejor posición
 *          second: Objeto CTF el que mejor ha quedado
 */
private fun calculaMejoresResultados(participaciones: List<Ctf>): MutableMap<Int, Pair<Int, Ctf>> {
    val participacionesByCTFId = participaciones.groupBy { it.id }
    var participacionesByGrupoId = participaciones.groupBy { it.grupoId }
    val mejoresCtfByGroupId = mutableMapOf<Int, Pair<Int, Ctf>>()
    participacionesByCTFId.values.forEach { ctfs ->
        val ctfsOrderByPuntuacion = ctfs.sortedBy { it.puntuacion }.reversed()
        participacionesByGrupoId.keys.forEach { grupoId ->
            val posicionNueva = ctfsOrderByPuntuacion.indexOfFirst { it.grupoId == grupoId }
            if (posicionNueva >= 0) {
                val posicionMejor = mejoresCtfByGroupId.getOrDefault(grupoId, null)
                if (posicionMejor != null) {
                    if (posicionNueva < posicionMejor.first)
                        mejoresCtfByGroupId.set(grupoId, Pair(posicionNueva, ctfsOrderByPuntuacion.get(posicionNueva)))
                } else
                    mejoresCtfByGroupId.set(grupoId, Pair(posicionNueva, ctfsOrderByPuntuacion.get(posicionNueva)))

            }
        }
    }
    return mejoresCtfByGroupId
}