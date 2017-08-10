(1 to 15).map {
  case num if num % 3 ==0 && num % 5 ==0 => println("Fuxx 3 and 5")
  case num if num % 3 ==0  => println("Fuxx 3")
  case num if num % 5 ==0 => println("Fuxx 5")
  case _ => println("FuzzBuzz")
}
