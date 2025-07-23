from gender_guesser_br import Genero

def sexoPorNome(nome):
    try:
        resultado = Genero(nome)()
        if resultado == "masculino":
            return "masculino"
        elif resultado == "feminino":
            return "feminino"
        else:
            return "desconhecido"
    except:
        return "desconhecido"
    
print(sexoPorNome("Joana"))