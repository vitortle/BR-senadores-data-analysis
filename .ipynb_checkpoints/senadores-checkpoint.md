```python
import requests #used to send request to the API
import json #used to read the API response 
import pandas as pd #used to... Ok... You know, right?!
import psycopg2 #used to work with PostgreSQL
from sqlalchemy import create_engine 
```


```python
#defining basic variables
base_url = 'https://dadosabertos.camara.leg.br/api/v2'
filter_url = '/deputados?ordem=ASC&ordenarPor=nome'

api_url = base_url + filter_url
api_url

#defining some basic functions
def getdata(link, explist):
    expenses = requests.get(link).json()
    return explist.append(expenses['dados'])

def nextlinkfromdict(pdf):
    for row in pdf['links']:
        if row['rel'] == "next":
            return row['href']
    return None

def nextlinkfromurl(link):
    pdf = requests.get(link).json()
    return nextlinkfromdict(pdf)
```


```python
#defining the headers and parameters of our request
headers  = {"chave-api-dados": "978907bd0bd33e12b18adc6b8f8927d1"}

params = {
  "pagina": 1
}
```


```python
response = requests.get(api_url)
deputadosdf = pd.DataFrame(response.json()['dados'])
```


```python
## Getting more information 
i = 0
depdf = []

for i in range(0, len(deputadosdf)):
    print("Starting dep " + str(i+1))
    dep_filter_url = base_url + '/deputados/' + str(deputadosdf['id'][i])
    depdf.append(requests.get(dep_filter_url).json())

#merging the useful columns to the best dataframe
depdados = pd.DataFrame(list(pd.DataFrame(depdf)['dados']))
deputadosdf = deputadosdf.merge(depdados[['id', 'nomeCivil', 'cpf', 'sexo', 'dataNascimento', 'ufNascimento', 'municipioNascimento', 'escolaridade']], on = 'id')
```

    Starting dep 1
    Starting dep 2
    Starting dep 3
    Starting dep 4
    Starting dep 5
    Starting dep 6
    Starting dep 7
    Starting dep 8
    Starting dep 9
    Starting dep 10
    Starting dep 11
    Starting dep 12
    Starting dep 13
    Starting dep 14
    Starting dep 15
    Starting dep 16
    Starting dep 17
    Starting dep 18
    Starting dep 19
    Starting dep 20
    Starting dep 21
    Starting dep 22
    Starting dep 23
    Starting dep 24
    Starting dep 25
    Starting dep 26
    Starting dep 27
    Starting dep 28
    Starting dep 29
    Starting dep 30
    Starting dep 31
    Starting dep 32
    Starting dep 33
    Starting dep 34
    Starting dep 35
    Starting dep 36
    Starting dep 37
    Starting dep 38
    Starting dep 39
    Starting dep 40
    Starting dep 41
    Starting dep 42
    Starting dep 43
    Starting dep 44
    Starting dep 45
    Starting dep 46
    Starting dep 47
    Starting dep 48
    Starting dep 49
    Starting dep 50
    Starting dep 51
    Starting dep 52
    Starting dep 53
    Starting dep 54
    Starting dep 55
    Starting dep 56
    Starting dep 57
    Starting dep 58
    Starting dep 59
    Starting dep 60
    Starting dep 61
    Starting dep 62
    Starting dep 63
    Starting dep 64
    Starting dep 65
    Starting dep 66
    Starting dep 67
    Starting dep 68
    Starting dep 69
    Starting dep 70
    Starting dep 71
    Starting dep 72
    Starting dep 73
    Starting dep 74
    Starting dep 75
    Starting dep 76
    Starting dep 77
    Starting dep 78
    Starting dep 79
    Starting dep 80
    Starting dep 81
    Starting dep 82
    Starting dep 83
    Starting dep 84
    Starting dep 85
    Starting dep 86
    Starting dep 87
    Starting dep 88
    Starting dep 89
    Starting dep 90
    Starting dep 91
    Starting dep 92
    Starting dep 93
    Starting dep 94
    Starting dep 95
    Starting dep 96
    Starting dep 97
    Starting dep 98
    Starting dep 99
    Starting dep 100
    Starting dep 101
    Starting dep 102
    Starting dep 103
    Starting dep 104
    Starting dep 105
    Starting dep 106
    Starting dep 107
    Starting dep 108
    Starting dep 109
    Starting dep 110
    Starting dep 111
    Starting dep 112
    Starting dep 113
    Starting dep 114
    Starting dep 115
    Starting dep 116
    Starting dep 117
    Starting dep 118
    Starting dep 119
    Starting dep 120
    Starting dep 121
    Starting dep 122
    Starting dep 123
    Starting dep 124
    Starting dep 125
    Starting dep 126
    Starting dep 127
    Starting dep 128
    Starting dep 129
    Starting dep 130
    Starting dep 131
    Starting dep 132
    Starting dep 133
    Starting dep 134
    Starting dep 135
    Starting dep 136
    Starting dep 137
    Starting dep 138
    Starting dep 139
    Starting dep 140
    Starting dep 141
    Starting dep 142
    Starting dep 143
    Starting dep 144
    Starting dep 145
    Starting dep 146
    Starting dep 147
    Starting dep 148
    Starting dep 149
    Starting dep 150
    Starting dep 151
    Starting dep 152
    Starting dep 153
    Starting dep 154
    Starting dep 155
    Starting dep 156
    Starting dep 157
    Starting dep 158
    Starting dep 159
    Starting dep 160
    Starting dep 161
    Starting dep 162
    Starting dep 163
    Starting dep 164
    Starting dep 165
    Starting dep 166
    Starting dep 167
    Starting dep 168
    Starting dep 169
    Starting dep 170
    Starting dep 171
    Starting dep 172
    Starting dep 173
    Starting dep 174
    Starting dep 175
    Starting dep 176
    Starting dep 177
    Starting dep 178
    Starting dep 179
    Starting dep 180
    Starting dep 181
    Starting dep 182
    Starting dep 183
    Starting dep 184
    Starting dep 185
    Starting dep 186
    Starting dep 187
    Starting dep 188
    Starting dep 189
    Starting dep 190
    Starting dep 191
    Starting dep 192
    Starting dep 193
    Starting dep 194
    Starting dep 195
    Starting dep 196
    Starting dep 197
    Starting dep 198
    Starting dep 199
    Starting dep 200
    Starting dep 201
    Starting dep 202
    Starting dep 203
    Starting dep 204
    Starting dep 205
    Starting dep 206
    Starting dep 207
    Starting dep 208
    Starting dep 209
    Starting dep 210
    Starting dep 211
    Starting dep 212
    Starting dep 213
    Starting dep 214
    Starting dep 215
    Starting dep 216
    Starting dep 217
    Starting dep 218
    Starting dep 219
    Starting dep 220
    Starting dep 221
    Starting dep 222
    Starting dep 223
    Starting dep 224
    Starting dep 225
    Starting dep 226
    Starting dep 227
    Starting dep 228
    Starting dep 229
    Starting dep 230
    Starting dep 231
    Starting dep 232
    Starting dep 233
    Starting dep 234
    Starting dep 235
    Starting dep 236
    Starting dep 237
    Starting dep 238
    Starting dep 239
    Starting dep 240
    Starting dep 241
    Starting dep 242
    Starting dep 243
    Starting dep 244
    Starting dep 245
    Starting dep 246
    Starting dep 247
    Starting dep 248
    Starting dep 249
    Starting dep 250
    Starting dep 251
    Starting dep 252
    Starting dep 253
    Starting dep 254
    Starting dep 255
    Starting dep 256
    Starting dep 257
    Starting dep 258
    Starting dep 259
    Starting dep 260
    Starting dep 261
    Starting dep 262
    Starting dep 263
    Starting dep 264
    Starting dep 265
    Starting dep 266
    Starting dep 267
    Starting dep 268
    Starting dep 269
    Starting dep 270
    Starting dep 271
    Starting dep 272
    Starting dep 273
    Starting dep 274
    Starting dep 275
    Starting dep 276
    Starting dep 277
    Starting dep 278
    Starting dep 279
    Starting dep 280
    Starting dep 281
    Starting dep 282
    Starting dep 283
    Starting dep 284
    Starting dep 285
    Starting dep 286
    Starting dep 287
    Starting dep 288
    Starting dep 289
    Starting dep 290
    Starting dep 291
    Starting dep 292
    Starting dep 293
    Starting dep 294
    Starting dep 295
    Starting dep 296
    Starting dep 297
    Starting dep 298
    Starting dep 299
    Starting dep 300
    Starting dep 301
    Starting dep 302
    Starting dep 303
    Starting dep 304
    Starting dep 305
    Starting dep 306
    Starting dep 307
    Starting dep 308
    Starting dep 309
    Starting dep 310
    Starting dep 311
    Starting dep 312
    Starting dep 313
    Starting dep 314
    Starting dep 315
    Starting dep 316
    Starting dep 317
    Starting dep 318
    Starting dep 319
    Starting dep 320
    Starting dep 321
    Starting dep 322
    Starting dep 323
    Starting dep 324
    Starting dep 325
    Starting dep 326
    Starting dep 327
    Starting dep 328
    Starting dep 329
    Starting dep 330
    Starting dep 331
    Starting dep 332
    Starting dep 333
    Starting dep 334
    Starting dep 335
    Starting dep 336
    Starting dep 337
    Starting dep 338
    Starting dep 339
    Starting dep 340
    Starting dep 341
    Starting dep 342
    Starting dep 343
    Starting dep 344
    Starting dep 345
    Starting dep 346
    Starting dep 347
    Starting dep 348
    Starting dep 349
    Starting dep 350
    Starting dep 351
    Starting dep 352
    Starting dep 353
    Starting dep 354
    Starting dep 355
    Starting dep 356
    Starting dep 357
    Starting dep 358
    Starting dep 359
    Starting dep 360
    Starting dep 361
    Starting dep 362
    Starting dep 363
    Starting dep 364
    Starting dep 365
    Starting dep 366
    Starting dep 367
    Starting dep 368
    Starting dep 369
    Starting dep 370
    Starting dep 371
    Starting dep 372
    Starting dep 373
    Starting dep 374
    Starting dep 375
    Starting dep 376
    Starting dep 377
    Starting dep 378
    Starting dep 379
    Starting dep 380
    Starting dep 381
    Starting dep 382
    Starting dep 383
    Starting dep 384
    Starting dep 385
    Starting dep 386
    Starting dep 387
    Starting dep 388
    Starting dep 389
    Starting dep 390
    Starting dep 391
    Starting dep 392
    Starting dep 393
    Starting dep 394
    Starting dep 395
    Starting dep 396
    Starting dep 397
    Starting dep 398
    Starting dep 399
    Starting dep 400
    Starting dep 401
    Starting dep 402
    Starting dep 403
    Starting dep 404
    Starting dep 405
    Starting dep 406
    Starting dep 407
    Starting dep 408
    Starting dep 409
    Starting dep 410
    Starting dep 411
    Starting dep 412
    Starting dep 413
    Starting dep 414
    Starting dep 415
    Starting dep 416
    Starting dep 417
    Starting dep 418
    Starting dep 419
    Starting dep 420
    Starting dep 421
    Starting dep 422
    Starting dep 423
    Starting dep 424
    Starting dep 425
    Starting dep 426
    Starting dep 427
    Starting dep 428
    Starting dep 429
    Starting dep 430
    Starting dep 431
    Starting dep 432
    Starting dep 433
    Starting dep 434
    Starting dep 435
    Starting dep 436
    Starting dep 437
    Starting dep 438
    Starting dep 439
    Starting dep 440
    Starting dep 441
    Starting dep 442
    Starting dep 443
    Starting dep 444
    Starting dep 445
    Starting dep 446
    Starting dep 447
    Starting dep 448
    Starting dep 449
    Starting dep 450
    Starting dep 451
    Starting dep 452
    Starting dep 453
    Starting dep 454
    Starting dep 455
    Starting dep 456
    Starting dep 457
    Starting dep 458
    Starting dep 459
    Starting dep 460
    Starting dep 461
    Starting dep 462
    Starting dep 463
    Starting dep 464
    Starting dep 465
    Starting dep 466
    Starting dep 467
    Starting dep 468
    Starting dep 469
    Starting dep 470
    Starting dep 471
    Starting dep 472
    Starting dep 473
    Starting dep 474
    Starting dep 475
    Starting dep 476
    Starting dep 477
    Starting dep 478
    Starting dep 479
    Starting dep 480
    Starting dep 481
    Starting dep 482
    Starting dep 483
    Starting dep 484
    Starting dep 485
    Starting dep 486
    Starting dep 487
    Starting dep 488
    Starting dep 489
    Starting dep 490
    Starting dep 491
    Starting dep 492
    Starting dep 493
    Starting dep 494
    Starting dep 495
    Starting dep 496
    Starting dep 497
    Starting dep 498
    Starting dep 499
    Starting dep 500
    Starting dep 501
    Starting dep 502
    Starting dep 503
    Starting dep 504
    Starting dep 505
    Starting dep 506
    Starting dep 507
    Starting dep 508
    Starting dep 509
    Starting dep 510
    Starting dep 511
    Starting dep 512
    Starting dep 513



```python
depdados = pd.DataFrame(list(pd.DataFrame(depdf)['dados']))
deputadosdf = deputadosdf.merge(depdados[['id', 'nomeCivil', 'cpf', 'sexo', 'dataNascimento', 'ufNascimento', 'municipioNascimento', 'escolaridade']], on = 'id')
```


```python
## Getting more information
i = 0
desdf = []
deputadosdf['despesas'] = None

for i in range(0, len(deputadosdf)):
    listdesp = []
    dep_filter_url = base_url + '/deputados/' + str(deputadosdf['id'][i]) + '/despesas'
    listdesp.append(getdata(dep_filter_url, listdesp))
    next_url = nextlinkfromurl(dep_filter_url)
    print("Starting dep " + str(i+1))
    while(next_url is not None):
        nextlist = requests.get(next_url).json()
        listdesp.append(nextlist['dados'])
        next_url = nextlinkfromdict(nextlist)
    
    deputadosdf['despesas'][i] = listdesp
#     time.sleep(1)

#merging the useful columns to the best dataframe
# deputadosdf
```

    Starting dep 1


    <ipython-input-7-bf84e5b6ad40>:17: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      deputadosdf['despesas'][i] = listdesp


    Starting dep 2
    Starting dep 3
    Starting dep 4
    Starting dep 5
    Starting dep 6
    Starting dep 7
    Starting dep 8
    Starting dep 9
    Starting dep 10
    Starting dep 11
    Starting dep 12
    Starting dep 13
    Starting dep 14
    Starting dep 15
    Starting dep 16
    Starting dep 17
    Starting dep 18
    Starting dep 19
    Starting dep 20
    Starting dep 21
    Starting dep 22
    Starting dep 23
    Starting dep 24
    Starting dep 25
    Starting dep 26
    Starting dep 27
    Starting dep 28
    Starting dep 29
    Starting dep 30
    Starting dep 31
    Starting dep 32
    Starting dep 33
    Starting dep 34
    Starting dep 35
    Starting dep 36
    Starting dep 37
    Starting dep 38
    Starting dep 39
    Starting dep 40
    Starting dep 41
    Starting dep 42
    Starting dep 43
    Starting dep 44
    Starting dep 45
    Starting dep 46
    Starting dep 47
    Starting dep 48
    Starting dep 49
    Starting dep 50
    Starting dep 51
    Starting dep 52
    Starting dep 53
    Starting dep 54
    Starting dep 55
    Starting dep 56
    Starting dep 57
    Starting dep 58
    Starting dep 59
    Starting dep 60
    Starting dep 61
    Starting dep 62
    Starting dep 63
    Starting dep 64
    Starting dep 65
    Starting dep 66
    Starting dep 67
    Starting dep 68
    Starting dep 69
    Starting dep 70
    Starting dep 71
    Starting dep 72
    Starting dep 73
    Starting dep 74
    Starting dep 75
    Starting dep 76
    Starting dep 77
    Starting dep 78
    Starting dep 79
    Starting dep 80
    Starting dep 81
    Starting dep 82
    Starting dep 83
    Starting dep 84
    Starting dep 85
    Starting dep 86
    Starting dep 87
    Starting dep 88
    Starting dep 89
    Starting dep 90
    Starting dep 91
    Starting dep 92
    Starting dep 93
    Starting dep 94
    Starting dep 95
    Starting dep 96
    Starting dep 97
    Starting dep 98
    Starting dep 99
    Starting dep 100
    Starting dep 101
    Starting dep 102
    Starting dep 103
    Starting dep 104
    Starting dep 105
    Starting dep 106
    Starting dep 107
    Starting dep 108
    Starting dep 109
    Starting dep 110
    Starting dep 111
    Starting dep 112
    Starting dep 113
    Starting dep 114
    Starting dep 115
    Starting dep 116
    Starting dep 117
    Starting dep 118
    Starting dep 119
    Starting dep 120
    Starting dep 121
    Starting dep 122
    Starting dep 123
    Starting dep 124
    Starting dep 125
    Starting dep 126
    Starting dep 127
    Starting dep 128
    Starting dep 129
    Starting dep 130
    Starting dep 131
    Starting dep 132
    Starting dep 133
    Starting dep 134
    Starting dep 135
    Starting dep 136
    Starting dep 137
    Starting dep 138
    Starting dep 139
    Starting dep 140
    Starting dep 141
    Starting dep 142
    Starting dep 143
    Starting dep 144
    Starting dep 145
    Starting dep 146
    Starting dep 147
    Starting dep 148
    Starting dep 149
    Starting dep 150
    Starting dep 151
    Starting dep 152
    Starting dep 153
    Starting dep 154
    Starting dep 155
    Starting dep 156
    Starting dep 157
    Starting dep 158
    Starting dep 159
    Starting dep 160
    Starting dep 161
    Starting dep 162
    Starting dep 163
    Starting dep 164
    Starting dep 165
    Starting dep 166
    Starting dep 167
    Starting dep 168
    Starting dep 169
    Starting dep 170
    Starting dep 171
    Starting dep 172
    Starting dep 173
    Starting dep 174
    Starting dep 175
    Starting dep 176
    Starting dep 177
    Starting dep 178
    Starting dep 179
    Starting dep 180
    Starting dep 181
    Starting dep 182
    Starting dep 183
    Starting dep 184
    Starting dep 185
    Starting dep 186
    Starting dep 187
    Starting dep 188
    Starting dep 189
    Starting dep 190
    Starting dep 191
    Starting dep 192
    Starting dep 193
    Starting dep 194
    Starting dep 195
    Starting dep 196
    Starting dep 197
    Starting dep 198
    Starting dep 199
    Starting dep 200
    Starting dep 201
    Starting dep 202
    Starting dep 203
    Starting dep 204
    Starting dep 205
    Starting dep 206
    Starting dep 207
    Starting dep 208
    Starting dep 209
    Starting dep 210
    Starting dep 211
    Starting dep 212
    Starting dep 213
    Starting dep 214
    Starting dep 215
    Starting dep 216
    Starting dep 217
    Starting dep 218
    Starting dep 219
    Starting dep 220
    Starting dep 221
    Starting dep 222
    Starting dep 223
    Starting dep 224
    Starting dep 225
    Starting dep 226
    Starting dep 227
    Starting dep 228
    Starting dep 229
    Starting dep 230
    Starting dep 231
    Starting dep 232
    Starting dep 233
    Starting dep 234
    Starting dep 235
    Starting dep 236
    Starting dep 237
    Starting dep 238
    Starting dep 239
    Starting dep 240
    Starting dep 241
    Starting dep 242
    Starting dep 243
    Starting dep 244
    Starting dep 245
    Starting dep 246
    Starting dep 247
    Starting dep 248
    Starting dep 249
    Starting dep 250
    Starting dep 251
    Starting dep 252
    Starting dep 253
    Starting dep 254
    Starting dep 255
    Starting dep 256
    Starting dep 257
    Starting dep 258
    Starting dep 259
    Starting dep 260
    Starting dep 261
    Starting dep 262
    Starting dep 263
    Starting dep 264
    Starting dep 265
    Starting dep 266
    Starting dep 267
    Starting dep 268
    Starting dep 269
    Starting dep 270
    Starting dep 271
    Starting dep 272
    Starting dep 273
    Starting dep 274
    Starting dep 275
    Starting dep 276
    Starting dep 277
    Starting dep 278
    Starting dep 279
    Starting dep 280
    Starting dep 281
    Starting dep 282
    Starting dep 283
    Starting dep 284
    Starting dep 285
    Starting dep 286
    Starting dep 287
    Starting dep 288
    Starting dep 289
    Starting dep 290
    Starting dep 291
    Starting dep 292
    Starting dep 293
    Starting dep 294
    Starting dep 295
    Starting dep 296
    Starting dep 297
    Starting dep 298
    Starting dep 299
    Starting dep 300
    Starting dep 301
    Starting dep 302
    Starting dep 303
    Starting dep 304
    Starting dep 305
    Starting dep 306
    Starting dep 307
    Starting dep 308
    Starting dep 309
    Starting dep 310
    Starting dep 311
    Starting dep 312
    Starting dep 313
    Starting dep 314
    Starting dep 315
    Starting dep 316
    Starting dep 317
    Starting dep 318
    Starting dep 319
    Starting dep 320
    Starting dep 321
    Starting dep 322
    Starting dep 323
    Starting dep 324
    Starting dep 325
    Starting dep 326
    Starting dep 327
    Starting dep 328
    Starting dep 329
    Starting dep 330
    Starting dep 331
    Starting dep 332
    Starting dep 333
    Starting dep 334
    Starting dep 335
    Starting dep 336
    Starting dep 337
    Starting dep 338
    Starting dep 339
    Starting dep 340
    Starting dep 341
    Starting dep 342
    Starting dep 343
    Starting dep 344
    Starting dep 345
    Starting dep 346
    Starting dep 347
    Starting dep 348
    Starting dep 349
    Starting dep 350
    Starting dep 351
    Starting dep 352
    Starting dep 353
    Starting dep 354
    Starting dep 355
    Starting dep 356
    Starting dep 357
    Starting dep 358
    Starting dep 359
    Starting dep 360
    Starting dep 361
    Starting dep 362
    Starting dep 363
    Starting dep 364
    Starting dep 365
    Starting dep 366
    Starting dep 367
    Starting dep 368
    Starting dep 369
    Starting dep 370
    Starting dep 371
    Starting dep 372
    Starting dep 373
    Starting dep 374
    Starting dep 375
    Starting dep 376
    Starting dep 377
    Starting dep 378
    Starting dep 379
    Starting dep 380
    Starting dep 381
    Starting dep 382
    Starting dep 383
    Starting dep 384
    Starting dep 385
    Starting dep 386
    Starting dep 387
    Starting dep 388
    Starting dep 389
    Starting dep 390
    Starting dep 391
    Starting dep 392
    Starting dep 393
    Starting dep 394
    Starting dep 395
    Starting dep 396
    Starting dep 397
    Starting dep 398
    Starting dep 399
    Starting dep 400
    Starting dep 401
    Starting dep 402
    Starting dep 403
    Starting dep 404
    Starting dep 405
    Starting dep 406
    Starting dep 407
    Starting dep 408
    Starting dep 409
    Starting dep 410
    Starting dep 411
    Starting dep 412
    Starting dep 413
    Starting dep 414
    Starting dep 415
    Starting dep 416
    Starting dep 417
    Starting dep 418
    Starting dep 419
    Starting dep 420
    Starting dep 421
    Starting dep 422
    Starting dep 423
    Starting dep 424
    Starting dep 425
    Starting dep 426
    Starting dep 427
    Starting dep 428
    Starting dep 429
    Starting dep 430
    Starting dep 431
    Starting dep 432
    Starting dep 433
    Starting dep 434
    Starting dep 435
    Starting dep 436
    Starting dep 437
    Starting dep 438
    Starting dep 439
    Starting dep 440
    Starting dep 441
    Starting dep 442
    Starting dep 443
    Starting dep 444
    Starting dep 445
    Starting dep 446
    Starting dep 447
    Starting dep 448
    Starting dep 449
    Starting dep 450
    Starting dep 451
    Starting dep 452
    Starting dep 453
    Starting dep 454
    Starting dep 455
    Starting dep 456
    Starting dep 457
    Starting dep 458
    Starting dep 459
    Starting dep 460
    Starting dep 461
    Starting dep 462
    Starting dep 463
    Starting dep 464
    Starting dep 465
    Starting dep 466
    Starting dep 467
    Starting dep 468
    Starting dep 469
    Starting dep 470
    Starting dep 471
    Starting dep 472
    Starting dep 473
    Starting dep 474
    Starting dep 475
    Starting dep 476
    Starting dep 477
    Starting dep 478
    Starting dep 479
    Starting dep 480
    Starting dep 481
    Starting dep 482
    Starting dep 483
    Starting dep 484
    Starting dep 485
    Starting dep 486
    Starting dep 487
    Starting dep 488
    Starting dep 489
    Starting dep 490
    Starting dep 491
    Starting dep 492
    Starting dep 493
    Starting dep 494
    Starting dep 495
    Starting dep 496
    Starting dep 497
    Starting dep 498
    Starting dep 499
    Starting dep 500
    Starting dep 501
    Starting dep 502
    Starting dep 503
    Starting dep 504
    Starting dep 505
    Starting dep 506
    Starting dep 507
    Starting dep 508
    Starting dep 509
    Starting dep 510
    Starting dep 511
    Starting dep 512
    Starting dep 513



```python
deputadosdf
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>uri</th>
      <th>nome</th>
      <th>siglaPartido</th>
      <th>uriPartido</th>
      <th>siglaUf</th>
      <th>idLegislatura</th>
      <th>urlFoto</th>
      <th>email</th>
      <th>nomeCivil</th>
      <th>cpf</th>
      <th>sexo</th>
      <th>dataNascimento</th>
      <th>ufNascimento</th>
      <th>municipioNascimento</th>
      <th>escolaridade</th>
      <th>despesas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>204554</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/depu...</td>
      <td>Abílio Santana</td>
      <td>PL</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/part...</td>
      <td>BA</td>
      <td>56</td>
      <td>https://www.camara.leg.br/internet/deputado/ba...</td>
      <td>dep.abiliosantana@camara.leg.br</td>
      <td>JOSE ABILIO SILVA DE SANTANA</td>
      <td>36607606504</td>
      <td>M</td>
      <td>1965-02-13</td>
      <td>BA</td>
      <td>Salvador</td>
      <td>Superior Incompleto</td>
      <td>[[{'ano': 2021, 'mes': 5, 'tipoDespesa': 'MANU...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>204521</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/depu...</td>
      <td>Abou Anni</td>
      <td>PSL</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/part...</td>
      <td>SP</td>
      <td>56</td>
      <td>https://www.camara.leg.br/internet/deputado/ba...</td>
      <td>dep.abouanni@camara.leg.br</td>
      <td>PAULO SERGIO ABOU ANNI</td>
      <td>08496582841</td>
      <td>M</td>
      <td>1966-11-06</td>
      <td>SP</td>
      <td>São Paulo</td>
      <td>Superior</td>
      <td>[[{'ano': 2021, 'mes': 8, 'tipoDespesa': 'MANU...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>204379</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/depu...</td>
      <td>Acácio Favacho</td>
      <td>PROS</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/part...</td>
      <td>AP</td>
      <td>56</td>
      <td>https://www.camara.leg.br/internet/deputado/ba...</td>
      <td>dep.acaciofavacho@camara.leg.br</td>
      <td>ACÁCIO DA SILVA FAVACHO NETO</td>
      <td>74287028287</td>
      <td>M</td>
      <td>1983-09-28</td>
      <td>AP</td>
      <td>Macapá</td>
      <td>Superior</td>
      <td>[[{'ano': 2021, 'mes': 4, 'tipoDespesa': 'MANU...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>204560</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/depu...</td>
      <td>Adolfo Viana</td>
      <td>PSDB</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/part...</td>
      <td>BA</td>
      <td>56</td>
      <td>https://www.camara.leg.br/internet/deputado/ba...</td>
      <td>dep.adolfoviana@camara.leg.br</td>
      <td>ADOLFO VIANA DE CASTRO NETO</td>
      <td>80123848504</td>
      <td>M</td>
      <td>1981-02-02</td>
      <td>BA</td>
      <td>Salvador</td>
      <td>Superior</td>
      <td>[[{'ano': 2021, 'mes': 4, 'tipoDespesa': 'COMB...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>204528</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/depu...</td>
      <td>Adriana Ventura</td>
      <td>NOVO</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/part...</td>
      <td>SP</td>
      <td>56</td>
      <td>https://www.camara.leg.br/internet/deputado/ba...</td>
      <td>dep.adrianaventura@camara.leg.br</td>
      <td>ADRIANA MIGUEL VENTURA</td>
      <td>12519851813</td>
      <td>F</td>
      <td>1969-03-06</td>
      <td>SP</td>
      <td>São Paulo</td>
      <td>Doutorado</td>
      <td>[[{'ano': 2021, 'mes': 6, 'tipoDespesa': 'MANU...</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>508</th>
      <td>178889</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/depu...</td>
      <td>Zé Carlos</td>
      <td>PT</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/part...</td>
      <td>MA</td>
      <td>56</td>
      <td>https://www.camara.leg.br/internet/deputado/ba...</td>
      <td>dep.zecarlos@camara.leg.br</td>
      <td>JOSÉ CARLOS NUNES JÚNIOR</td>
      <td>10009728368</td>
      <td>M</td>
      <td>1955-05-26</td>
      <td>MA</td>
      <td>São Luís</td>
      <td>Superior</td>
      <td>[[{'ano': 2021, 'mes': 3, 'tipoDespesa': 'MANU...</td>
    </tr>
    <tr>
      <th>509</th>
      <td>204559</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/depu...</td>
      <td>Zé Neto</td>
      <td>PT</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/part...</td>
      <td>BA</td>
      <td>56</td>
      <td>https://www.camara.leg.br/internet/deputado/ba...</td>
      <td>dep.zeneto@camara.leg.br</td>
      <td>JOSÉ CERQUEIRA DE SANTANA NETO</td>
      <td>38247186500</td>
      <td>M</td>
      <td>1964-03-30</td>
      <td>BA</td>
      <td>Feira de Santana</td>
      <td>Pós-Graduação</td>
      <td>[[{'ano': 2020, 'mes': 12, 'tipoDespesa': 'MAN...</td>
    </tr>
    <tr>
      <th>510</th>
      <td>160632</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/depu...</td>
      <td>Zé Silva</td>
      <td>SOLIDARIEDADE</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/part...</td>
      <td>MG</td>
      <td>56</td>
      <td>https://www.camara.leg.br/internet/deputado/ba...</td>
      <td>dep.zesilva@camara.leg.br</td>
      <td>JOSÉ SILVA SOARES</td>
      <td>43422780653</td>
      <td>M</td>
      <td>1963-05-11</td>
      <td>MG</td>
      <td>Iturama</td>
      <td>Pós-Graduação</td>
      <td>[[{'ano': 2021, 'mes': 7, 'tipoDespesa': 'MANU...</td>
    </tr>
    <tr>
      <th>511</th>
      <td>204517</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/depu...</td>
      <td>Zé Vitor</td>
      <td>PL</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/part...</td>
      <td>MG</td>
      <td>56</td>
      <td>https://www.camara.leg.br/internet/deputado/ba...</td>
      <td>dep.zevitor@camara.leg.br</td>
      <td>JOSE VITOR DE RESENDE AGUIAR</td>
      <td>07284044608</td>
      <td>M</td>
      <td>1984-11-01</td>
      <td>MG</td>
      <td>Araguari</td>
      <td>Superior</td>
      <td>[[{'ano': 2021, 'mes': 4, 'tipoDespesa': 'MANU...</td>
    </tr>
    <tr>
      <th>512</th>
      <td>160592</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/depu...</td>
      <td>Zeca Dirceu</td>
      <td>PT</td>
      <td>https://dadosabertos.camara.leg.br/api/v2/part...</td>
      <td>PR</td>
      <td>56</td>
      <td>https://www.camara.leg.br/internet/deputado/ba...</td>
      <td>dep.zecadirceu@camara.leg.br</td>
      <td>JOSÉ CARLOS BECKER DE OLIVEIRA E SILVA</td>
      <td>03098871946</td>
      <td>M</td>
      <td>1978-06-21</td>
      <td>PR</td>
      <td>Umuarama</td>
      <td>Superior</td>
      <td>[[{'ano': 2021, 'mes': 3, 'tipoDespesa': 'MANU...</td>
    </tr>
  </tbody>
</table>
<p>513 rows × 17 columns</p>
</div>




```python
def insert_into_table(dataframe, table):
    sql = "INSERT INTO deputados() VALUES(%s)"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,vendor_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

db_string = "postgresql://yetusbwb@kesavan.db.elephantsql.com:5432/yetusbwb"
db = create_engine(db_string).raw_connection()
# conn = psycopg2.connect("dbname='yetusbwb' user='yetusbwb' host='kesavan.db.elephantsql.com'");
# cursor = conn.cursor()
# deputadosdf.drop(['despesas'], axis=1).to_sql('deputado', con=db, schema="yetusbwb",)
# conn.commit()
# cursor.close()
# conn.close()
```


    ---------------------------------------------------------------------------

    UndefinedTable                            Traceback (most recent call last)

    ~/anaconda3/envs/senadores/lib/python3.8/site-packages/pandas/io/sql.py in execute(self, *args, **kwargs)
       2055         try:
    -> 2056             cur.execute(*args, **kwargs)
       2057             return cur


    UndefinedTable: relation "sqlite_master" does not exist
    LINE 1: SELECT name FROM sqlite_master WHERE type='table' AND name=?...
                             ^


    
    The above exception was the direct cause of the following exception:


    DatabaseError                             Traceback (most recent call last)

    <ipython-input-169-98f21eb7e03b> in <module>
          3 # conn = psycopg2.connect("dbname='yetusbwb' user='yetusbwb' host='kesavan.db.elephantsql.com'");
          4 # cursor = conn.cursor()
    ----> 5 deputadosdf.drop(['despesas'], axis=1).to_sql('deputado', con=db, schema="yetusbwb",)
          6 # conn.commit()
          7 # cursor.close()


    ~/anaconda3/envs/senadores/lib/python3.8/site-packages/pandas/core/generic.py in to_sql(self, name, con, schema, if_exists, index, index_label, chunksize, dtype, method)
       2867         from pandas.io import sql
       2868 
    -> 2869         sql.to_sql(
       2870             self,
       2871             name,


    ~/anaconda3/envs/senadores/lib/python3.8/site-packages/pandas/io/sql.py in to_sql(frame, name, con, schema, if_exists, index, index_label, chunksize, dtype, method, engine, **engine_kwargs)
        715         )
        716 
    --> 717     pandas_sql.to_sql(
        718         frame,
        719         name,


    ~/anaconda3/envs/senadores/lib/python3.8/site-packages/pandas/io/sql.py in to_sql(self, frame, name, if_exists, index, index_label, schema, chunksize, dtype, method, **kwargs)
       2223             dtype=dtype,
       2224         )
    -> 2225         table.create()
       2226         table.insert(chunksize, method)
       2227 


    ~/anaconda3/envs/senadores/lib/python3.8/site-packages/pandas/io/sql.py in create(self)
        854 
        855     def create(self):
    --> 856         if self.exists():
        857             if self.if_exists == "fail":
        858                 raise ValueError(f"Table '{self.name}' already exists.")


    ~/anaconda3/envs/senadores/lib/python3.8/site-packages/pandas/io/sql.py in exists(self)
        838 
        839     def exists(self):
    --> 840         return self.pd_sql.has_table(self.name, self.schema)
        841 
        842     def sql_schema(self):


    ~/anaconda3/envs/senadores/lib/python3.8/site-packages/pandas/io/sql.py in has_table(self, name, schema)
       2234         query = f"SELECT name FROM sqlite_master WHERE type='table' AND name={wld};"
       2235 
    -> 2236         return len(self.execute(query, [name]).fetchall()) > 0
       2237 
       2238     def get_table(self, table_name: str, schema: str | None = None):


    ~/anaconda3/envs/senadores/lib/python3.8/site-packages/pandas/io/sql.py in execute(self, *args, **kwargs)
       2066 
       2067             ex = DatabaseError(f"Execution failed on sql '{args[0]}': {exc}")
    -> 2068             raise ex from exc
       2069 
       2070     @staticmethod


    DatabaseError: Execution failed on sql 'SELECT name FROM sqlite_master WHERE type='table' AND name=?;': relation "sqlite_master" does not exist
    LINE 1: SELECT name FROM sqlite_master WHERE type='table' AND name=?...
                             ^




```python
deputadosdf.drop(['despesas'], axis=1)
```


    ---------------------------------------------------------------------------

    NameError                                 Traceback (most recent call last)

    <ipython-input-1-deaf5010acdc> in <module>
    ----> 1 deputadosdf.drop(['despesas'], axis=1)
    

    NameError: name 'deputadosdf' is not defined



```python
pdf = requests.get(dep_filter_url).json()

def getnextlink(link):
    pdf = requests.get(link).json()
    for row in pdf['links']:
        if row['rel'] == "next":
            return row['href']
    return None
        
getnextlink('https://dadosabertos.camara.leg.br/api/v2/deputados/204554/despesas?pagina=10') is None
```




    True




```python
df = requests.get("https://dadosabertos.camara.leg.br/api/v2/deputados/204554/despesas?pagina=10&itens=15").json()
```


```python
pdf = pd.DataFrame(df['links'])
if(len(pdf[pdf['rel'] == 'next']) == 0):
    print('True')
else:
    print('False')
```

    True

