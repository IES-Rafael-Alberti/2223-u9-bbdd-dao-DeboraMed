import java.sql.DriverManager
import java.sql.ResultSet
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

}
// 1º comando añade participacion de un grupo (insert en ctf) consulta sobre ctf con mejor puntucion de ese grupo
//(update) de mejor posicion
fun insertaGrupo(grupos:Grupo){

    //conexion con la BBDD
    val conn = DriverManager.getConnection(jdbcUrl, userName, password)
    //sentencia insert
    val insertSqlString = ("insert into grupos(grupoid, grupodesc) values(?,'?')")

    var preparedStatement = conn.prepareStatement(insertSqlString) // Uso de PreparedStatement

    preparedStatement.setInt(1, grupos.grupoid)
    preparedStatement.setInt(2, grupos.mejorCtfId)

    val insertCount = preparedStatement.executeUpdate()

    //comprueba si se ha realizado el insert
    if (insertCount > 0) {
        println("Se ha insertado la fila correctamente")
    } else {
        println("La inserción de fila ha fallado")
    }

    preparedStatement.close()
}




// 2º comando elimina participacion de un grupo (insert en ctf) consulta sobre ctf con mejor puntucion de ese grupo
//(update) de mejor posicion

// 3º select del grupo dado, sino info de todo





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