set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;



drop table dc_dwd.ywcp_temp_1;
create table dc_dwd.ywcp_temp_1 as
select user_number,
       scene_id,
       notifyid,
       dts_collect_time,
       dts_kaf_time,
       dts_kaf_offset,
       dts_kaf_part,
       operate_type,
       operate_time,
       operate_type_desc,
       trail_seq,
       trail_rba,
       id,
       crttime,
       entid,
       callid,
       callnumber,
       keyno,
       keyname,
       ordernum,
       sourceindication,
       userid,
       a.date_id,
       part_id,
       row_number() over (partition by user_number order by ordernum) as rn1
from (select user_number, scene_id, date_id
      from dc_src_rt.eval_send_sms_log
      where scene_id in (
                         '28731575546528',
                         '28730574017184',
                         '28731575605920',
                         '28506387527328',
                         '28731575659680',
                         '28593732463776',
                         '28731575707808',
                         '28731575762080',
                         '28731575814816',
                         '28731575883424',
                         '28731575961760',
                         '28731576015520',
                         '28731576086688',
                         '28731576167584',
                         '28731576243360',
                         '28731576308384',
                         '28731576383136',
                         '28731576453280',
                         '28731576507040',
                         '28731576558240',
                         '28731576618144',
                         '28731576667296',
                         '28731576717984',
                         '28731576769696',
                         '28731576823968',
                         '28731576876704',
                         '28731576932000',
                         '28731576989856',
                         '28732058761888',
                         '28732058817184',
                         '28732058872992',
                         '28732058924192',
                         '28732058981024',
                         '29018555604640',
                         '29026323751584',
                         '29026355042464',
                         '29026382751392',
                         '28732167641760',
                         '28732167696032',
                         '28732167749280',
                         '28732167799456',
                         '28732167854240',
                         '28732167911072',
                         '28732167986848',
                         '28732168039584',
                         '28732168094368',
                         '28732168144544',
                         '28732168199328',
                         '28732169163424',
                         '28732169212576',
                         '28732169264800',
                         '28732169326752',
                         '28732169395360',
                         '28732169447584',
                         '28732169501856',
                         '28732169560736',
                         '28732169611936',
                         '28732169660576',
                         '28732169712288',
                         '28732169795232',
                         '28732169845920',
                         '28732169899168',
                         '28732169955488',
                         '28732170008736',
                         '28978375289504',
                         '28979597683360',
                         '28732342637728',
                         '28979697065632',
                         '29018520668320',
                         '28732342692512',
                         '28732342890656',
                         '28732342943904',
                         '28732342998688',
                         '28732343055008',
                         '29018629209248',
                         '28732343126176',
                         '28732343227552',
                         '28732343278752',
                         '28732343344800',
                         '28732343400096',
                         '28732343451296',
                         '28732343502496',
                         '28732343750304',
                         '28732343801504',
                         '28732343852704',
                         '28732492667552',
                         '28732492812960',
                         '28732492870816',
                         '28732492933792',
                         '28732493004448',
                         '28732493069472',
                         '28732493126304',
                         '28732493183648',
                         '28732493244576',
                         '28732686607008',
                         '28732686669984',
                         '28732687691936',
                         '28732687741088',
                         '28732687789728',
                         '28732687835296',
                         '28732687893152',
                         '28732687944352',
                         '28732687997088',
                         '28732688049312',
                         '28732688103072',
                         '28732688159392',
                         '28732688214176',
                         '28732688272032',
                         '28732688322720',
                         '28732688370848',
                         '28732688430240',
                         '28732688485024',
                         '28732688546464',
                         '28732688599200',
                         '28732688648352',
                         '28732688694432',
                         '28732688746656',
                         '28732688796320',
                         '28732688847008',
                         '28732688894112',
                         '28732688942240',
                         '28732688995488',
                         '28733138814112',
                         '28733138904224',
                         '28733138948768',
                         '28733138998944',
                         '28733139042464',
                         '29020668339872',
                         '29024854688416',
                         '29026480394912',
                         '28733139092128',
                         '28733139140768',
                         '28733139188384',
                         '28733139250336',
                         '28733139304096',
                         '28733139376800',
                         '29018682337952',
                         '28733139428512',
                         '28733139475616',
                         '28733139531424',
                         '28733139592352',
                         '28733139642016',
                         '28733139688608',
                         '28733139734176',
                         '28733388905120',
                         '28733388956832',
                         '28733389009056',
                         '28733389061280',
                         '28979795360928',
                         '28979830317728',
                         '28733389105824',
                         '28733389152416',
                         '28733389194400',
                         '28733389236896',
                         '28733389277344',
                         '28733389318816',
                         '28733389360800',
                         '28733389425312',
                         '28733389465760',
                         '28733389501600',
                         '28733389540512',
                         '28733389579424',
                         '28733389617824',
                         '28733389655200',
                         '28733468599456',
                         '28733468647072',
                         '28733468698272',
                         '28979867992224',
                         '28733468745376',
                         '28733468788896',
                         '28733468838048',
                         '28733468894368',
                         '29026514652320',
                         '29026541380256',
                         '28733469921952',
                         '29026634371744',
                         '29026663333024',
                         '29026691725472',
                         '28733469977248',
                         '28733470021280',
                         '28733470063776',
                         '28735516709024',
                         '28979910373024',
                         '28735516791968',
                         '28733583312544',
                         '28733583363744',
                         '28733583399584',
                         '28981663470240',
                         '28733583450272',
                         '28981701289120',
                         '28733583491232',
                         '28733583531168',
                         '28733583569568',
                         '28733583606944',
                         '28733583659168',
                         '28733583702688',
                         '28733583746720',
                         '28733583784608',
                         '28733583827104',
                         '28733583867040',
                         '28733583910560',
                         '28733583945376',
                         '28733583983776',
                         '28733584023712',
                         '28733584062624',
                         '28733584100512',
                         '28733584139424',
                         '28981772562080',
                         '28986883880096',
                         '28986911004320',
                         '28993933770400',
                         '28994034643104',
                         '28994065887904',
                         '29026767686304',
                         '28733737996448',
                         '28733738051232',
                         '28733738098336',
                         '28733738173088',
                         '28733738216096',
                         '28733738288800',
                         '28733738329248',
                         '28733738372768',
                         '28733738433696',
                         '28733738485408',
                         '28733738529440',
                         '28733738588832',
                         '28733738791584',
                         '28733738843296',
                         '28733738919584',
                         '28733738960544',
                         '28493936353952',
                         '28501621124256',
                         '28501661976736'
          )) a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id >= '20240326') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id >= '20240326'
              and notifyid in (
                               'a273b5fbb52c4074995b6677bc269679',
                               'b365680e04634dff93201d3324c3cbc8',
                               '1a16de6f02e54ef0adaa51a924dbff4e',
                               'e1d0292d670a466082e97cda521d0a29',
                               'bd80601f40a345a79a4600c590d1a3fc',
                               '75f08d9768c2436e8120e3e900c3a4c2',
                               '0326f69d3b6049e985c2da67bd188848',
                               '3d4985f95d0340ed8f1037e34c6df67b',
                               '0869e60846d1452e80f620d14c57a58f',
                               '44da9ac254ee4066916484ee6f6606dd',
                               'f23ff4c6a2ba4c269b00955b3b02ef3b',
                               '60d40651f2124cdbb07e8124e1cbb5cf',
                               '5faa7eb02a4046c4a12f57b0733cc258',
                               '6a2d7cca8e964282b8297822e8c659d6',
                               '1a4ba1154fe64490a5bc884835c5e849',
                               '1e3a0612a977488eac0b408a858c20c6',
                               'b051a06639d64b54885b4425aa9a5d33',
                               '2b4f04709f754a32ba4ca3f3121b80bb',
                               '756f0cc20b754ac7982272276ba5735c',
                               'e914720e862745069cf4db4f12de41a9',
                               'bf2b62d945214ffaac08c7827e3817cd',
                               '68d9b26110bd477c8aaaa3d9cdb41b38',
                               '7fcb127f9be74e84a15ef4c217c0d624',
                               '9ef22392a79040ca854deb53e8c2ea9d',
                               'bbc9e62d2d654ff88c638dd571ea0283',
                               'ce087d49abd044c9b823e214458adecc',
                               '26e70505e79249868dfe6cbc5d62e096',
                               '47534d0d7b424d22bd088a25116fcc19',
                               '404da47ae8584bac912b5a95a8a2323b',
                               '4d06ae1eba3a43d09a084e18511c3e12',
                               '1f0bba67c5ce4208bc829bcebaa2fa55',
                               '1f2f95ae00c44fea8d8f5e8558da2175',
                               '1c239e77a97545c580c2156332b87ab8',
                               'f1b420dc89b24d52b8fd69deb9cc28b8',
                               'cae948fa56f546f49375af6b79ae8f8a',
                               '7686d66e844846ae99189f48b04fb783',
                               '3b74ffb42f0348909fe9860249958835',
                               '69933d598caf4146abfc0a8dc25a8903',
                               'c526ef5aead64d4c8c98fe417ef6a48f',
                               '1dd66059c14a414eaa44d3b137e5653e',
                               '9e36c3ae6df64c148746b7e4aab0a127',
                               '1c8b9da20d194e568052a3e40424ab6f',
                               '10125b54753d49e8856f4b2a3e0a6178',
                               '91f935bf98a74d7ba3310ee47f1de1da',
                               '7e756ad5b4284991a8b2c2563dad265d',
                               '78b8a567c14842038d5ce54083b8d320',
                               'c1e5c449d5f44157b8e8afc9387a88de',
                               '9f6ea49d08224ec887e2f48427d27e1f',
                               'b891e41aedf643c1941fd7998f496fd1',
                               '1992732b426b4b868fe8cd814145594b',
                               '7d2009a108074a12b2f4d583c2b28e21',
                               '714eb1128fb54248bf5ff0432163b6cc',
                               '3992dfe7bef548bd9dd6dbaa055706a1',
                               '7dd618ef3d7046baaed658e50bf3d7c3',
                               '6c7e885a124f45a7935be29997d3547b',
                               '8ce45011fc5f42bb97efc8b233bda4b1',
                               '4b26f04e4968492f88be7360a5933478',
                               'a34624c8ca824304bf0b1a9467d13be6',
                               '5c628fa5b05d4d2db070ada09cf7abb5',
                               'ab0fca3682a84368a6a13367d4381b4f',
                               '4db7b3e656204b8ea8c23fe53a00329c',
                               '78d99c590d2d479b9e62939ee456675a',
                               '08d792f4c493499d8d55822a0405eba8',
                               'fd2cab4bb5994a8ea76023b64a2d089c',
                               'c1e6379d20eb44bab2877c536ea96e79',
                               '7dc50f9832a94c23add597b9e81f71f5',
                               'a4446da869ad46ab833b519983e7abca',
                               '6651c01eeb8645b6972912d4c047f664',
                               '708905ce529c4f0b8bec3b55c8182789',
                               '58b36e1a05af422c82402d855c6213b5',
                               '1a17a1ea18844d7b8dda9ce59f8033eb',
                               '676ee8ea7b5349519c55a8a8ab34e5ca',
                               '1d86b1893a0d46da856d9c41093614b8',
                               '07b6277397144636a5a9f09daabde5c2',
                               '7bca1391eb6347b2b9d5d5378029f022',
                               'e2412af3a60d4a3c841ad16116c5780a',
                               'b7b5dda1a44b42fd87e01fe267af2c4b',
                               '966087417e5042ef86d6b1b8b18cdbc1',
                               '81f33a6b538e4085a072a2aaa19842ec',
                               '5dae37f824744aa79544dd6130482f4b',
                               'f1aac2c04c6742b486930fe58f3bbf1f',
                               '904e8570cec64dea8adb27279cc53643',
                               '8ef16e10fab249c287ca1ef700e7b64a',
                               '6c468badcee74a05a0140238edbea9c5',
                               '4f75c64b7164423bbe0334cbbbad1570',
                               '6c093c90ae9749759e21e15019766265',
                               '667b8cb51e464dc6ab8090dc67770ecb',
                               '83ffd602e4c440749cc6bbad224451e5',
                               '713592180cee4ede8c54b3370019867f',
                               'f10a2bbdc2224792b1c4a6806670858f',
                               '4b4c08d0c4504132b33751648aacf1ad',
                               '85ba8edba4dd490ba43f53a375c7e66e',
                               '9b311dd79e834e908ebd808b1be80cc8',
                               'b297656d5e9443f0adc52bd9c1a1ce90',
                               '11b27306ca1942f896449b1065e4adcd',
                               '00922b23f8e64d9eb69b462375cf4666',
                               '9891a4ff9daa4941bf7115dbb8c3c7d0',
                               'baaa43d66b1f473aba0425b641d512e7',
                               '7f23aa64aaa740c393d75bb7e6c0204c',
                               'c55f19a7667449428b09fe495fa6275e',
                               'eaa6e529ecca4858b5b962e287125042',
                               'e903e61e49aa4e86bd32b96304a5e453',
                               '03dec298f9484a1ea3bdaa163994eee1',
                               '5807353c65784289a24ee18566dee04e',
                               '38d65b61889a4e7eac53b550f91efc91',
                               '1a1e44f0853744da9a2e7d21c3b9eac1',
                               'b3323197a9ac432e878fa96ab2d422d9',
                               '4134a3314280409fac55d723f7163dce',
                               '015a5f51235a4261be47a0022a2fa014',
                               '1973f2dacdde43a4ba98788fd9e3fa6b',
                               'fb0d5f68e7d54caca72e12ea0b5ed01a',
                               '1a32281d3d1d4cd5920918eb9924bf26',
                               '052d60c4d3c44374a6f9b3453e1df1c6',
                               '60d4f6108ad1439c8b7e22610d7e00de',
                               'ae1d017c04154395b3fdb63d1e482d37',
                               'bb42b31df6ce4a719936f027adab81ef',
                               '8c1a1546ab4444b2a73c69d89a58b0ea',
                               '963c3f89e59d43f4ab981f614fe356fd',
                               'a36a29a8374540a78e74d7680991076e',
                               '6d64c285a1d848ba8e0e35418f4760cb',
                               'e580d7aee6fc4012a88737fe297367a3',
                               '6aa2930a049648c192b699b5f898a7f8',
                               '1270d6df80874f87b348598311982f7b',
                               '052d989a00a54d24a5725299d1d729ce',
                               '4ffb33aa6c994169a646b1b9c576440b',
                               '8feffa89349a4c598285b4bbc0636d06',
                               'ad7c7e103f4d4349ae4355cc08c7672d',
                               '2427727c5ae347438a48ff9af4c690a0',
                               '87753e884e264a28b025b74288ec0365',
                               '39f89fa8860045bd85501eeee08f0819',
                               '67a3b386431b4fc5bd5f1a5c27509c0b',
                               '4feb7633f3b24d95a61e99fb681d09d5',
                               '3c1c170cc3a5420b9fd481ebef23b9ea',
                               'eb00af0dd92544128a3a0f5b590c73dd',
                               '3a1102cd26f5488cbdee7428b38596ed',
                               'e9980942c6994bd3b921fe0060531d5e',
                               'ad2d34d6507c40c39ff06e4a8bfb401c',
                               '4dbebe6b2aa44ee98817999f2300c12d',
                               'ad7c7e103f4d4349ae4355cc08c7672d',
                               '1c10a5813ed74eb5be1574506315af43',
                               '0dcaa82ed50945c2939ae37507a80590',
                               '93143fa4f98e453fbf71c6ac6a1cad81',
                               'f00b0b76ed6445c796d7ced89697aa22',
                               '8e44d9d5d16f495e9387bc86f0d3e450',
                               '07f7ff93d8c24c978db622bcbf8e6c58',
                               '10f6ed9137f148c5a4ca22e684ff5008',
                               '974c00b98b9f4ff3992ccd35a5f603f8',
                               '843d77c9f6594778902b06a0bdeaada6',
                               'e1229f3429e749dd9348382936699434',
                               'ae151f5578bb45ea80687964f0c37004',
                               '078203796c0443e88f70b0e66d90e0e5',
                               'dbf6979478404bff9866a38b6e760361',
                               '046f99def4544c9992b168fe276f9b92',
                               'fe86e1003fb14e38bc5a469f510c0d33',
                               '55c39f56009546c8aa9b1239c7e844d3',
                               'c7ea227da34346a6b79dd8c9845310ea',
                               'f2ea3a9122d940bbbd12eeaddd1252cd',
                               'b60c56ce9dd447bb919df1628c77b912',
                               '6ec2d36f50324a2c87435ccaee7cac60',
                               '4ed7c1f633674386a6b2640e69e118ef',
                               '191bf93b370b4844a741632816d8a0c8',
                               '1cf80d6db4e64a5f923d4fc519b02768',
                               'e4fe6d67549443be8ef7a1ae205a7895',
                               'dd7b76df87394e1ab138067f6810c3f0',
                               '1ba2b313bfbb40d8b23bc0da29ce2d2f',
                               '11bf2f2939df462c8af0b1bf5995615c',
                               '37f6c2c7f6ca472b8df8a1cf95a4d6a1',
                               '3776593e8e834026aa135600ff891051',
                               'e8ca287d4b4c4f92ace30de52b48c0b0',
                               'cfd43cc1e95847fa9cb39469936128e1',
                               '1cd615170bf641b39b9f9a2c99fc3de2',
                               'ccec8ce7f80f427d9f8e095c35c1fc70',
                               '49e57b3a17ef4dd58c8aff430b57ef49',
                               '0e2a788753e0474da2a8438be0d8db33',
                               '09cb8eb9ebe342488f5607d033412a99',
                               'b44f5ad7aa5847da852470af3c76f811',
                               '231cca1fed2746f29948bd77be789ac5',
                               '3ad79ec7304d47bba1a8377deaeb793d',
                               'b06f939ec55d4023802593843751040d',
                               'c3825514c12043f28ff18aae61f3ff77',
                               '4a187d53f10e4364b84c43e01a9c8988',
                               '1ca4d76d0fb44f919eb537acbb08f463',
                               '025fe12a94e2433faf2e6365e818ca36',
                               'c453a143bdc7434d92fa80c375017ff3',
                               'd7ba707f88b446abbea5bdb96dde12a8',
                               '44368b431d1a40248eccaaf8e9b20ace',
                               'df1c85c2c5a34e63b371975761143adf',
                               '681cb35f46c4443ba8130861b77a5e23',
                               'fc32d648c4b44b099a9cf5f9df3e49e3',
                               '096a9d3fd8734cdbad7d07ecdd12f3f7',
                               '4fb9777dd06e4fe7b3764ad12033c645',
                               '9a00a2633f4b4bd28eed0ab6d6b3b410',
                               '8e2d783f02c441f78d84f4d70e26c882',
                               'a48155e8be574d69afdcb41c48b509ea',
                               'ae97f9e4207348ae8d5f9bb4045e2c7c',
                               '1677a67b83fb4e4ca437d295fd45b032',
                               '2d86b294f1d34f69bc42b333f5264aa5',
                               '9ec5fea599c6452680a5321afe01cabd',
                               '72426fb46acb4d6daa4673885825d4c9',
                               'dfbb6e5345284ede92c6ef5b5908e69c',
                               'b36beb3e57504258ab47ffbee27b78ec',
                               'c0ad81b6e99d4fbab2d185e73edb3587',
                               '173e29d4710a4317a0130b5f1db28796',
                               '4482f628069544b7935dcafe02ea29d9',
                               '021398b0210644b79df7f8416999f135',
                               '29dd6c04e52f482aa1e7b428f935a167',
                               '84d85891e7d64fa5895415e7bd0e5300',
                               '0527609e941c4f88a340e9082b5ffde9',
                               '0733435d270c453880056c13910615e6',
                               '37f13e23866b41eb91b517ce2b11759e',
                               '0da92afb97ca4bd1891e4d6d325a2b99',
                               'eec2b0db214e4261a2e3828b5e00dc67',
                               'b628cc700f7546aeb159192ed6cd826d',
                               '7e734e654efa433c81bf3bf43b7a5a6c',
                               'c6f0796a21a24bf794e1c42e2d56a715',
                               '0907e7c3185e4533929ae951e5ac1dec',
                               '3e09d83a8b26459a9db8ca35d139f8fb',
                               '47429a23380d45b087f1e3eb14e8e9e2',
                               '07cf5db897474e619fae11102eeb96ce',
                               'b6b035d6d00b41739624fa79cafcb2ff',
                               '90145656be234cd3bd5885713f41e571',
                               '92285a67e77b4a7f9e05e5b8e5f849f3',
                               'd961f4f54452400689d30ef03bba33a6',
                               'dfff778a89f247b99bea0c0ab54b5cf3',
                               'c334555484ca421281b710b87f13899d',
                               'b733108659f84a4391db4e160624d00b',
                               '2dc21d27dd4d45d0983f37493ce6a3a4',
                               'b3e18a2af9f34be59a9e7d00eac3cf49',
                               '577ac7ffb6f04066ab74b4e9977c90af',
                               'b9876217291741edbff8483418bc34e2',
                               '14906103e3254924b4dd75153b906289',
                               'e9c4d636d09d44ec980d8ee759569ea0',
                               '95c7cb9d6c7140e4b9ad8b6f8c3e0531'
                )) b
           on a.callid = b.id) b
     on a.user_number = b.callnumber;

select *
from dc_dwd.ywcp_temp_1
where keyno is not null;



drop table dc_dwd.ywcp_temp_2;
create table dc_dwd.ywcp_temp_2 as
select user_number, collect_list(keyno) as source
from dc_dwd.ywcp_temp_1
group by user_number;

drop table dc_dwd.ywcp_temp_3;
create table dc_dwd.ywcp_temp_3 as
select a.user_number,
       crttime,
       scene_id,
       notifyid,
       date_id,
       split(concat_ws(',', source), ',')[0] as aj_1,
       split(concat_ws(',', source), ',')[1] as aj_2,
       split(concat_ws(',', source), ',')[2] as aj_3,
       split(concat_ws(',', source), ',')[3] as aj_4,
       split(concat_ws(',', source), ',')[4] as aj_5
from dc_dwd.ywcp_temp_2 a
         left join(select * from dc_dwd.ywcp_temp_1 where rn1 = 1) b on a.user_number = b.user_number;



drop table dc_Dwd.ywcp_mx_temp;
--明细表
create table dc_Dwd.ywcp_mx_temp as
select sf_name,
       ds_name,
       if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type) as scen_type,
       id,
       scenename,
       user_number,
       crttime,
       aj_1,
       aj_2,
       aj_3,
       aj_4,
       aj_5,
       case
           when aj_1 = '1' then '满意'
           when aj_1 = '2' then '一般'
           when aj_1 = '3' then '不满意'
           else null
           end                                                            as yymy,
       case
           when aj_1 in ('2', '3') and aj_2 = '1' then '电话无法打通、接通'
           when aj_1 in ('2', '3') and aj_2 = '2' then '通话经常中断'
           when aj_1 in ('2', '3') and aj_2 = '3' then '通话有杂音、听不清'
           when aj_1 in ('2', '3') and aj_2 = '4' then '其他'
           else null end                                                  as yywt,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when aj_1 = '1' then case
                                            when aj_2 = '1' then '手机流量'
                                            when aj_2 = '2' then '宽带WIFI'
                                            when aj_2 = '3' then '不清楚'
                                            else null end
                   when aj_1 in ('2', '3') then case
                                                    when aj_3 = '1' then '手机流量'
                                                    when aj_3 = '2' then '宽带WIFI'
                                                    when aj_3 = '3' then '不清楚'
                                                    else null end end
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') and aj_1 is not null
               then '手机流量'
           else null end
                                                                          as swfs,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when aj_1 = '1' and aj_2 in ('1', '2') then case
                                                                   when aj_3 = '1' then '满意'
                                                                   when aj_3 = '2' then '一般'
                                                                   when aj_3 = '3' then '不满意'
                                                                   else null end
                   when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('1', '2') then case
                                                                                                            when aj_4 = '1'
                                                                                                                then '满意'
                                                                                                            when aj_4 = '2'
                                                                                                                then '一般'
                                                                                                            when aj_4 = '3'
                                                                                                                then '不满意'
                                                                                                            else null end end
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when aj_1 = '1' then
                       case
                           when aj_2 = '1' then '满意'
                           when aj_2 = '2' then '一般'
                           when aj_2 = '3' then '不满意' end
                   when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') then case
                                                                                     when aj_3 = '1'
                                                                                         then '满意'
                                                                                     when aj_3 = '2'
                                                                                         then '一般'
                                                                                     when aj_3 = '3'
                                                                                         then '不满意' end end
           end
                                                                          as swmy,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when aj_1 = '1' and aj_2 in ('1', '2') and aj_3 in ('2', '3') then case
                                                                                          when aj_4 = '1' then '无法上网'
                                                                                          when aj_4 = '2' then '网速慢'
                                                                                          when aj_4 = '3'
                                                                                              then '上网卡顿、不稳定'
                                                                                          else null end
                   when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('1', '2') and
                        aj_4 in ('2', '3')
                       then case
                                when aj_5 = '1'
                                    then '无法上网'
                                when aj_5 = '2'
                                    then '网速慢'
                                when aj_5 = '3'
                                    then '上网卡顿、不稳定'
                                when aj_5 = '4' then '其他'
                                else null end end
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when aj_1 = '1' and aj_2 in ('2', '3') then
                       case
                           when aj_3 = '1' then '无法上网'
                           when aj_3 = '2' then '网速慢'
                           when aj_3 = '3' then '上网卡顿、不稳定'
                           when aj_3 = '4' then '其他'
                           end
                   when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('2', '3') then case
                                                                                                            when aj_4 = '1'
                                                                                                                then '无法上网'
                                                                                                            when aj_4 = '2'
                                                                                                                then '网速慢'
                                                                                                            when aj_4 = '3'
                                                                                                                then '上网卡顿、不稳定'
                                                                                                            when aj_4 = '4'
                                                                                                                then '其他' end
                   end end                                                as swwt
from (select sf_name,
             ds_name,
             ptext as scen_type,
             id,
             scenename,
             user_number,
             crttime,
             aj_1,
             aj_2,
             aj_3,
             aj_4,
             aj_5
      from dc_dwd.ywcp_temp_3 aa
               left join (select serial_number,
                                 province_code,
                                 eparchy_code,
                                 scenename,
                                 a.id,
                                 b.scen_type as ptext,
                                 text,
                                 usertype,
                                 month_id,
                                 prov_id,
                                 update_time
                          from dc_dwd.group_source_all_1 a
                                   left join dc_dwd.xdc_scen_type b on a.id = b.id) bb
                         on aa.user_number = bb.serial_number
               left join dc_dwd.scence_prov_code cc
                         on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code) dd;

