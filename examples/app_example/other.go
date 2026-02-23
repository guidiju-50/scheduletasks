// Outro ficheiro: guarda a referência a app e invoca app.NotifySchedule() quando necessário.
package main

var appRef *App

// SetApp guarda app para ser usada neste ficheiro. main.go chama SetApp(app) após criar app.
func SetApp(app *App) {
	appRef = app
}

// NotifyScheduleNow invoca app.NotifySchedule() a partir de other.go.
// main.go (ou um handler) chama NotifyScheduleNow() quando a agenda mudar; a invocação real ocorre aqui.
func NotifyScheduleNow() {
	if appRef != nil {
		appRef.NotifySchedule()
	}
}




